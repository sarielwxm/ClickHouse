#include <Storages/MergeTree/MergePlainMergeTreeTask.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Interpreters/TransactionLog.h>
#include <Common/ProfileEventsScope.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadFuzzer.h>

#include <Storages/StorageTSearchMonitorMergeTree.h>

#include <Storages/StorageReplicatedMergeTree.h>

#include <Storages/Kafka/KafkaSettings.h>
#include <Storages/MessageQueueSink.h>
#include <atomic>
#include <cppkafka/cppkafka.h>
#include <Databases/DatabaseMonitor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


StorageID MergePlainMergeTreeTask::getStorageID() const
{
    return storage.getStorageID();
}

void MergePlainMergeTreeTask::onCompleted()
{
    bool delay = state == State::SUCCESS;
    task_result_callback(delay);
}

bool MergePlainMergeTreeTask::executeStep()
{
    /// All metrics will be saved in the thread_group, including all scheduled tasks.
    /// In profile_counters only metrics from this thread will be saved.
    ProfileEventsScope profile_events_scope(&profile_counters);

    /// Make out memory tracker a parent of current thread memory tracker
    std::optional<ThreadGroupSwitcher> switcher;
    if (merge_list_entry)
    {
        switcher.emplace((*merge_list_entry)->thread_group);
    }

    switch (state)
    {
        case State::NEED_PREPARE :
        {
            prepare();
            state = State::NEED_EXECUTE;
            return true;
        }
        case State::NEED_EXECUTE :
        {
            try
            {
                if (merge_task->execute())
                    return true;

                state = State::NEED_FINISH;
                return true;
            }
            catch (...)
            {
                write_part_log(ExecutionStatus::fromCurrentException("", true));
                throw;
            }
        }
        case State::NEED_FINISH :
        {
            finish();

            state = State::SUCCESS;
            return false;
        }
        case State::SUCCESS:
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Task with state SUCCESS mustn't be executed again");
        }
    }
}


void MergePlainMergeTreeTask::prepare()
{
    future_part = merge_mutate_entry->future_part;
    stopwatch_ptr = std::make_unique<Stopwatch>();

    task_context = createTaskContext();
    merge_list_entry = storage.getContext()->getMergeList().insert(
        storage.getStorageID(),
        future_part,
        task_context);

    write_part_log = [this] (const ExecutionStatus & execution_status)
    {
        auto profile_counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(profile_counters.getPartiallyAtomicSnapshot());
        merge_task.reset();
        storage.writePartLog(
            PartLogElement::MERGE_PARTS,
            execution_status,
            stopwatch_ptr->elapsed(),
            future_part->name,
            new_part,
            future_part->parts,
            merge_list_entry.get(),
            std::move(profile_counters_snapshot));
    };

    transfer_profile_counters_to_initial_query = [this, query_thread_group = CurrentThread::getGroup()] ()
    {
        if (query_thread_group)
        {
            auto task_thread_group = (*merge_list_entry)->thread_group;
            auto task_counters_snapshot = task_thread_group->performance_counters.getPartiallyAtomicSnapshot();

            auto & query_counters = query_thread_group->performance_counters;
            for (ProfileEvents::Event i = ProfileEvents::Event(0); i < ProfileEvents::end(); ++i)
                query_counters.incrementNoTrace(i, task_counters_snapshot[i]);
        }
    };

    merge_task = storage.merger_mutator.mergePartsToTemporaryPart(
            future_part,
            metadata_snapshot,
            merge_list_entry.get(),
            {} /* projection_merge_list_element */,
            table_lock_holder,
            time(nullptr),
            task_context,
            merge_mutate_entry->tagger->reserved_space,
            deduplicate,
            deduplicate_by_columns,
            cleanup,
            storage.merging_params,
            txn);
}


void MergePlainMergeTreeTask::finish()
{
    new_part = merge_task->getFuture().get();

    MergeTreeData::Transaction transaction(storage, txn.get());
    storage.merger_mutator.renameMergedTemporaryPart(new_part, future_part->parts, txn, transaction);
    transaction.commit();

//    auto process_table = [&](const std::string& process_table_name)
//    {
//        if (storage.getStorageID().getTableName() == process_table_name && storage.getStorageID().getDatabaseName() == "system")
//        {
//            MergeTreePartInfo new_part_info(
//                new_part->info.partition_id, new_part->info.min_block, new_part->info.max_block, new_part->info.level);
//            StoragePtr monitor_table = DatabaseCatalog::instance().getDatabase("s3")->tryGetTable(process_table_name, storage.getContext());
//            if (!monitor_table)
//                return;
//            auto monitor = std::dynamic_pointer_cast<StorageTSearchMonitorMergeTree>(monitor_table);
//
//            auto storage_settings_ptr = monitor->getSettings();
//            auto source_metadata_snapshot = monitor->getInMemoryMetadataPtr();
//
//            Stopwatch watch;
//            ProfileEventsScope profile_events_scope;
//
//            bool zero_copy_enabled = storage_settings_ptr->allow_remote_fs_zero_copy_replication
//                || storage.getSettings()->allow_remote_fs_zero_copy_replication;
//            IDataPartStorage::ClonePartParams clone_params{
//                .copy_instead_of_hardlink = storage_settings_ptr->always_use_copy_instead_of_hardlinks
//                    || (zero_copy_enabled && new_part->isStoredOnRemoteDiskWithZeroCopySupport()),
//                .metadata_version_to_write = metadata_snapshot->getMetadataVersion()};
//            auto [cloned_part, part_lock] = monitor->cloneAndLoadDataPart(
//                new_part,
//                "tmp_replace_from_",
//                new_part_info,
//                monitor->getInMemoryMetadataPtr(),
//                clone_params,
//                monitor->getContext()->getReadSettings(),
//                monitor->getContext()->getWriteSettings(),
//                false /*must_on_same_disk*/);
//
//            String data_path = cloned_part->getDataPartStorage().getFullRootPath();
//            String metadata_path = DatabaseCatalog::instance().getDatabase("s3")->getMetadataPath();
//            String table_name = monitor_table->getStorageID().getTableName();
//            String database_name = monitor_table->getStorageID().getDatabaseName();
//
//            auto database_monitor = std::dynamic_pointer_cast<DatabaseMonitor>(DatabaseCatalog::instance().getDatabase(database_name));
//            auto remote_database_name = database_monitor->getConfig().remote_database_name;
//            String disk_name = monitor->getDisks().front()->getName();
//            MergeTreeData::DataPartsVector parts = monitor->getAllDataPartsVector();
//
//            ReplicatedMergeTreeLogEntryData entry;
//            {
//                entry.type = ReplicatedMergeTreeLogEntryData::REPLACE_RANGE;
//                entry.create_time = time(nullptr);
//                entry.replace_range_entry = std::make_shared<ReplicatedMergeTreeLogEntryData::ReplaceRangeEntry>();
//
//                auto & entry_replace = *entry.replace_range_entry;
//
//                entry_replace.data_path = data_path;
//                entry_replace.metadata_path = metadata_path + table_name + ".sql";
//                for (const auto & part : parts)
//                {
//                    entry_replace.src_part_names.emplace_back(part->name);
//                }
//                entry_replace.new_part_names.emplace_back(new_part->name);
//                entry_replace.from_database = remote_database_name;
//                entry_replace.from_table = table_name;
//                entry_replace.disk_name = disk_name;
//                entry_replace.columns_version = -1;
//            }
//
//            MergeTreeData::Transaction monitor_transaction(*monitor, NO_TRANSACTION_RAW);
//            monitor->renameTempPartAndReplace(cloned_part, monitor_transaction);
//            monitor_transaction.commit();
//
//            MergeTreeData::MutableDataPartsVector cloned_parts;
//            cloned_parts.emplace_back(cloned_part);
//            PartLog::addNewParts(
//                monitor->getContext(), PartLog::createPartLogEntries(cloned_parts, watch.elapsed(), profile_events_scope.getSnapshot()));
//
//            monitor->produceMessageToKafka(entry.toString(), 1, 0);
//        }
//    };
//
//    const auto & config = storage.getContext()->getConfigRef();
//
//    String prefix_config = "tsearch_monitor.tables";
//    Poco::Util::AbstractConfiguration::Keys keys;
//    config.keys(prefix_config, keys);
//    for (auto & key : keys)
//    {
//        String value = config.getString(prefix_config + "." + key);
//        process_table(value);
//    }

    auto process_table = [&](const std::string& process_table_name)
    {
        if (storage.getStorageID().getTableName() == process_table_name && storage.getStorageID().getDatabaseName() != "system")
        {
            MergeTreePartInfo new_part_info(
                new_part->info.partition_id, new_part->info.min_block, new_part->info.max_block, new_part->info.level);

            auto monitor = dynamic_cast<StorageTSearchMonitorMergeTree*>(&storage);

            String data_path = new_part->getDataPartStorage().getFullRootPath();

            String table_name = storage.getStorageID().getTableName();
            String database_name = storage.getStorageID().getDatabaseName();
            String metadata_path = DatabaseCatalog::instance().getDatabase(database_name)->getMetadataPath();

            auto database_monitor = std::dynamic_pointer_cast<DatabaseMonitor>(DatabaseCatalog::instance().getDatabase(database_name));
            auto remote_database_name = database_monitor->getConfig().remote_database_name;
            String disk_name = storage.getDisks().front()->getName();
            MergeTreeData::DataPartsVector parts = monitor->getAllDataPartsVector();

            ReplicatedMergeTreeLogEntryData entry;
            {
                entry.type = ReplicatedMergeTreeLogEntryData::REPLACE_RANGE;
                entry.create_time = time(nullptr);
                entry.replace_range_entry = std::make_shared<ReplicatedMergeTreeLogEntryData::ReplaceRangeEntry>();

                auto & entry_replace = *entry.replace_range_entry;

                entry_replace.data_path = data_path;
                entry_replace.metadata_path = metadata_path + table_name + ".sql";
                for (const auto & part : parts)
                {
                    entry_replace.src_part_names.emplace_back(part->name);
                }
                entry_replace.new_part_names.emplace_back(new_part->name);
                entry_replace.from_database = remote_database_name;
                entry_replace.from_table = table_name;
                entry_replace.disk_name = disk_name;
                entry_replace.columns_version = -1;
            }

            monitor->produceMessageToKafka(entry.toString(), 1, 0);
        }
    };
    process_table("metric_log");
    process_table("asynchronous_metric_log");
    process_table("query_log");
    process_table("text_log");


    ThreadFuzzer::maybeInjectSleep();
    ThreadFuzzer::maybeInjectMemoryLimitException();

    write_part_log({});
    StorageMergeTree::incrementMergedPartsProfileEvent(new_part->getType());
    transfer_profile_counters_to_initial_query();

    if (auto txn_ = txn_holder.getTransaction())
    {
        /// Explicitly commit the transaction if we own it (it's a background merge, not OPTIMIZE)
        TransactionLog::instance().commitTransaction(txn_, /* throw_on_unknown_status */ false);
        ThreadFuzzer::maybeInjectSleep();
        ThreadFuzzer::maybeInjectMemoryLimitException();
    }

}

ContextMutablePtr MergePlainMergeTreeTask::createTaskContext() const
{
    auto context = Context::createCopy(storage.getContext());
    context->makeQueryContextForMerge(*storage.getSettings());
    auto queryId = getQueryId();
    context->setCurrentQueryId(queryId);
    context->setBackgroundOperationTypeForContext(ClientInfo::BackgroundOperationType::MERGE);
    return context;
}

}
