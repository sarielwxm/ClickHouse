#include <filesystem>
#include <Databases/DatabaseMonitor.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabaseOnDisk.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/TablesLoader.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/formatAST.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>

#include <IO/S3/Client.h>
#include <IO/S3/Credentials.h>
#include <IO/S3Settings.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/ReadBufferFromS3.h>
#include <Common/Macros.h>

#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/ObjectStorages/ObjectStorageIterator.h>
#include <Disks/ObjectStorages/ObjectStorageFactory.h>
#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>

#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/ObjectStorages/S3/S3Capabilities.h>
#include <memory>
#include <Disks/SingleDiskVolume.h>
#include <Disks/ObjectStorages/S3/diskSettings.h>

#include <Common/MultiVersion.h>
#include <Common/ObjectStorageKeyGenerator.h>

#include <aws/s3/model/ListObjectsV2Request.h>

#include <boost/algorithm/string.hpp>

#include <Storages/Kafka/KafkaConsumer.h>
#include <Storages/Kafka/KafkaProducer.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Storages/Kafka/KafkaSource.h>
#include <Storages/MessageQueueSink.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/StorageTSearchMonitorMergeTree.h>

#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>

#include <atomic>
#include <cppkafka/cppkafka.h>
#include <chrono>
#include <thread>

namespace fs = std::filesystem;


namespace CurrentMetrics
{
extern const Metric DatabaseOnDiskThreads;
extern const Metric DatabaseOnDiskThreadsActive;
extern const Metric DatabaseOnDiskThreadsScheduled;
}

namespace DB
{

static const std::unordered_set<std::string_view> optional_configuration_keys = {
    "url",
    "access_key_id",
    "secret_access_key",
    "no_sign_request",
    "remote_database_name"
};

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int TABLE_ALREADY_EXISTS;
extern const int INCORRECT_FILE_NAME;
extern const int UNKNOWN_DATABASE;
extern const int FILE_DOESNT_EXIST;
extern const int NO_ELEMENTS_IN_CONFIG;
}

std::shared_ptr<S3::Client> makeS3Client(
    const S3::URI & s3_uri,
    const String & access_key_id,
    const String & secret_access_key,
    const S3Settings & settings,
    const ContextPtr & context)
{
    Aws::Auth::AWSCredentials credentials(access_key_id, secret_access_key);
    HTTPHeaderEntries headers;
    if (access_key_id.empty())
    {
        credentials = Aws::Auth::AWSCredentials(settings.auth_settings.access_key_id, settings.auth_settings.secret_access_key);
        headers = settings.auth_settings.headers;
    }

    const auto & request_settings = settings.request_settings;
    const Settings & global_settings = context->getGlobalContext()->getSettingsRef();
    const Settings & local_settings = context->getSettingsRef();

    S3::PocoHTTPClientConfiguration client_configuration = S3::ClientFactory::instance().createClientConfiguration(
        settings.auth_settings.region,
        context->getRemoteHostFilter(),
        static_cast<unsigned>(global_settings.s3_max_redirects),
        static_cast<unsigned>(global_settings.s3_retry_attempts),
        global_settings.enable_s3_requests_logging,
        /* for_disk_s3 = */ false,
        request_settings.get_request_throttler,
        request_settings.put_request_throttler,
        s3_uri.uri.getScheme());

    client_configuration.endpointOverride = s3_uri.endpoint;
    client_configuration.maxConnections = static_cast<unsigned>(global_settings.s3_max_connections);
    /// Increase connect timeout
    client_configuration.connectTimeoutMs = 10 * 1000;
    /// Requests in backups can be extremely long, set to one hour
    client_configuration.requestTimeoutMs = 60 * 60 * 1000;
    client_configuration.http_keep_alive_timeout = S3::DEFAULT_KEEP_ALIVE_TIMEOUT;
    client_configuration.http_keep_alive_max_requests = S3::DEFAULT_KEEP_ALIVE_MAX_REQUESTS;

    S3::ClientSettings client_settings{
        .use_virtual_addressing = s3_uri.is_virtual_hosted_style,
        .disable_checksum = local_settings.s3_disable_checksum,
        .gcs_issue_compose_request = context->getConfigRef().getBool("s3.gcs_issue_compose_request", false),
        .is_s3express_bucket = S3::isS3ExpressEndpoint(s3_uri.endpoint),
    };

    return S3::ClientFactory::instance().create(
        client_configuration,
        client_settings,
        credentials.GetAWSAccessKeyId(),
        credentials.GetAWSSecretKey(),
        settings.auth_settings.server_side_encryption_customer_key_base64,
        settings.auth_settings.server_side_encryption_kms_config,
        std::move(headers),
        S3::CredentialsConfiguration{
            settings.auth_settings.use_environment_credentials,
            settings.auth_settings.use_insecure_imds_request,
            settings.auth_settings.expiration_window_seconds,
            settings.auth_settings.no_sign_request});
}



DatabaseMonitor::DatabaseMonitor(const String & name, const String & metadata_path_, UUID uuid, const Configuration& config_, ContextPtr local_context)
    : DatabaseMonitor(name, metadata_path_, "data/" + escapeForFileName(name) + "/", uuid, config_, "DatabaseMonitor (" + name + ")", local_context)
{
    if (local_context->getConfigRef().getBool("monitor", false))
    {
        auto broker = local_context->getConfigRef().getString("kafka_broker", "");
        auto topic = local_context->getConfigRef().getString("kafka_consumer_topic", "");
        auto consumer_group = local_context->getConfigRef().getString("kafka_consumer_group", "");

        auto max_batch_size = local_context->getConfigRef().getInt64("max_batch_size", 1);
        auto poll_timeout = local_context->getConfigRef().getInt64("poll_timeout", 100);
        auto intermediate_commit = local_context->getConfigRef().getBool("intermediate_commit", true);

        if (broker.empty() || topic.empty() || consumer_group.empty())
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "There is no Kafka configuration in server config.");

        Names topics = {topic};
        LoggerPtr log(getLogger("kafka"));

        kafka_consumer = std::make_shared<KafkaConsumer>(
            log, max_batch_size, poll_timeout, intermediate_commit, stopped, topics);
        cppkafka::Configuration consumer_config;
        consumer_config.set("metadata.broker.list", broker);
        consumer_config.set("group.id", consumer_group);

        kafka_consumer->createConsumer(consumer_config);
        kafka_consumer->subscribe();
        task = getContext()->getSchedulePool().createTask("MonitorTask", [this] { threadFunc(); });
        if (task)
            task->activateAndSchedule();
    }
}

DatabaseMonitor::DatabaseMonitor(const String & name, const String & metadata_path_, const String & data_path_, UUID uuid, const Configuration& config_, const String & logger, ContextPtr local_context)
    : DatabaseWithOwnTablesBase(name, logger, local_context)
    , config(config_)
    , metadata_path(metadata_path_)
    , data_path(data_path_)
    , db_uuid(uuid)
{
    assert(db_uuid != UUIDHelpers::Nil);
    fs::create_directories(local_context->getPath() + data_path);
    fs::create_directories(metadata_path);

    s3_uri = S3::URI(fs::path(config.url_prefix));
    client = makeS3Client(
        s3_uri,
        config.access_key_id.value(),
        config.secret_access_key.value(),
        s3_settings,
        local_context);
}

void DatabaseMonitor::threadFunc()
{
    if (shutdown_called)
        return;

    try
    {
        LOG_DEBUG(log, "Started consuming Kafka messages");

        if (consumeKafkaMessages())
        {
            /// Reset the reschedule interval.
            reschedule_processing_interval_ms = polling_min_timeout_ms;
        }
        else
        {
            /// Increase the reschedule interval.
            reschedule_processing_interval_ms += polling_backoff_ms;
        }

        LOG_DEBUG(log, "Stopped consuming Kafka messages");
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to process data: {}", getCurrentExceptionMessage(true));
    }

    if (!shutdown_called)
    {
        LOG_TRACE(log, "Reschedule processing thread in {} ms", reschedule_processing_interval_ms);
        task->scheduleAfter(reschedule_processing_interval_ms);
    }
}

/// database 应该是监控节点对应新建的
/// entry 中应该携带 管控应该在监控集群创建的db
/// 没有这个db的时候就要抛出异常，说明管控问题，没有这个数据库
/// 找到数据库之后找表，如果不在内存，说明表没加载，从metadata加载schema，然后从part list恢复
/// 最后加载entry part

/// log entry 里有足够的信息告诉你 这张监控表应该在监控集群的哪一个db被加载，metadata的位置，data的位置，part list用来充当快照

/// log entry 里需要有监控集群的db，metadata的disk，data的disk --> 对于s3来说，就是bucket

/// 监控db可以在业务db创建的时候指定（唯一性？）
/// 业务monitor db的元数据bucket应该都是一样的

/// 监控可以从每一份log entry恢复

bool DatabaseMonitor::consumeKafkaMessages()
{
    auto buildAndAddDataPart = [&](
                                   const std::shared_ptr<StorageTSearchMonitorMergeTree>& table,
                                   const std::string& part_name,
                                   const std::string& data_part_path,
                                   const std::string& disk_name,
                                   ContextPtr context) {
        auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, context->getDisk(disk_name));
        MergeTreeData::MutableDataPartPtr data_part;
        MergeTreeDataPartBuilder builder(*table, part_name, volume, data_part_path, part_name);

        data_part = builder.withPartFormatFromDisk().build();
        data_part->version.setCreationTID(Tx::PrehistoricTID, nullptr);
        data_part->is_temp = true;
        data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::PRESERVE_BLOBS;
        data_part->modification_time = time(nullptr);
        data_part->loadColumnsChecksumsIndexes(true, false);

        MergeTreeData::Transaction part_transaction(*table, NO_TRANSACTION_RAW);
        {
            auto part_lock = table->lockParts();
            StorageTSearchMonitorMergeTree::DataPartsVector covered_parts;
            table->addPart(data_part, part_transaction, part_lock, &covered_parts);
        }
        part_transaction.commit();

        return data_part_path;
    };

    auto read_buffer_ptr = kafka_consumer->consume();
    if (read_buffer_ptr)
    {
        std::string message;
        while (!read_buffer_ptr->eof())
        {
            char c;
            read_buffer_ptr->readStrict(c);
            message.push_back(c);
        }

        std::cout << "Received message: " << message << std::endl;

        Coordination::Stat stat;
        MergeTreeDataFormatVersion format_version;

        auto log_entry = ReplicatedMergeTreeLogEntry::parse(message, stat, format_version);

        auto database = DatabaseCatalog::instance().getDatabase(log_entry->replace_range_entry->from_database);
        if (!database)
            throw Exception(
                ErrorCodes::UNKNOWN_DATABASE, "Database {} is not exists", log_entry->replace_range_entry->from_database);
        StoragePtr monitor_table = database->tryGetTable(log_entry->replace_range_entry->from_table, getContext());
        if (!monitor_table)
        {
            // Assuming all metadata is in same bucket
            auto ast = parseQueryFromMetadata(
                getLogger("kafka"),
                getContext(),
                log_entry->replace_range_entry->metadata_path,
                /*throw_on_error*/ true,
                /*remove_empty*/ false);
            FunctionNameNormalizer::visit(ast.get());
            const auto & create_query = ast->as<const ASTCreateQuery &>();

            auto [table_name, table] = createTableFromAST(
                create_query,
                log_entry->replace_range_entry->from_database,
                "./",
                std::const_pointer_cast<Context>(getContext()),
                LoadingStrictnessLevel::ATTACH);

            try
            {
                std::lock_guard lock{mutex};
                attachTableUnlocked(table_name, table);
            }
            catch (...)
            {
                throw;
            }
            // attachTable(local_context, table_name, table, log_entry->replace_range_entry->data_path);
            monitor_table = database->tryGetTable(log_entry->replace_range_entry->from_table, getContext());
            auto rmt = std::dynamic_pointer_cast<StorageTSearchMonitorMergeTree>(monitor_table);
            for (const auto &src_part_name : log_entry->replace_range_entry->src_part_names)
            {
                buildAndAddDataPart(rmt, src_part_name, log_entry->replace_range_entry->data_path, log_entry->replace_range_entry->disk_name, getContext());
            }
        }
        auto rmt = std::dynamic_pointer_cast<StorageTSearchMonitorMergeTree>(monitor_table);

        buildAndAddDataPart(rmt, log_entry->replace_range_entry->new_part_names[0], log_entry->replace_range_entry->data_path, log_entry->replace_range_entry->disk_name, getContext());
    }
    return true;
}

DatabaseMonitor::Configuration DatabaseMonitor::parseArguments(ASTs engine_args, ContextPtr context_)
{
    Configuration result;

    if (auto named_collection = tryGetNamedCollectionWithOverrides(engine_args, context_))
    {
        auto & collection = *named_collection;

        validateNamedCollection(collection, {}, optional_configuration_keys);

        result.url_prefix = collection.getOrDefault<String>("url", "");
        result.remote_database_name = collection.getOrDefault<String>("remote_database_name", "");
        result.no_sign_request = collection.getOrDefault<bool>("no_sign_request", false);

        auto key_id = collection.getOrDefault<String>("access_key_id", "");
        auto secret_key = collection.getOrDefault<String>("secret_access_key", "");

        if (!key_id.empty())
            result.access_key_id = key_id;

        if (!secret_key.empty())
            result.secret_access_key = secret_key;
    }
    else
    {
        const std::string supported_signature =
            " - Monitor()\n"
            " - Monitor('url')\n"
            " - Monitor('url', 'NOSIGN')\n"
            " - Monitor('url', 'access_key_id', 'secret_access_key', 'remote_database_name')\n";
        const auto error_message =
            fmt::format("Engine DatabaseMonitor must have the following arguments signature\n{}", supported_signature);

        for (auto & arg : engine_args)
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context_);

        if (engine_args.size() > 4)
            throw Exception::createRuntime(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, error_message.c_str());

        if (engine_args.empty())
            return result;

        result.url_prefix = checkAndGetLiteralArgument<String>(engine_args[0], "url");
        result.remote_database_name = checkAndGetLiteralArgument<String>(engine_args[3], "remote_database_name");

        // url, NOSIGN
        if (engine_args.size() == 2)
        {
            auto second_arg = checkAndGetLiteralArgument<String>(engine_args[1], "NOSIGN");
            if (boost::iequals(second_arg, "NOSIGN"))
                result.no_sign_request = true;
            else
                throw Exception::createRuntime(ErrorCodes::BAD_ARGUMENTS, error_message.c_str());
        }

        // url, access_key_id, secret_access_key, remote_database_name
        if (engine_args.size() == 4)
        {
            auto key_id = checkAndGetLiteralArgument<String>(engine_args[1], "access_key_id");
            auto secret_key = checkAndGetLiteralArgument<String>(engine_args[2], "secret_access_key");

            if (key_id.empty() || secret_key.empty() || boost::iequals(key_id, "NOSIGN"))
                throw Exception::createRuntime(ErrorCodes::BAD_ARGUMENTS, error_message.c_str());

            result.access_key_id = key_id;
            result.secret_access_key = secret_key;
        }
    }

    return result;
}

bool DatabaseMonitor::checkUrl(const std::string & url, ContextPtr context_, bool throw_on_error) const
{
    try
    {
        S3::URI uri(url);
        context_->getGlobalContext()->getRemoteHostFilter().checkURL(uri.uri);
    }
    catch (...)
    {
        if (throw_on_error)
            throw;
        return false;
    }
    return true;
}

std::string DatabaseMonitor::getFullUrl(const std::string & name) const
{
    if (!config.url_prefix.empty())
        return fs::path(config.url_prefix) / name;

    return name;
}

ASTPtr DatabaseMonitor::getCreateDatabaseQuery() const
{
    const auto & settings = getContext()->getSettingsRef();
    ParserCreateQuery parser;

    std::string creation_args;
    creation_args += fmt::format("'{}'", config.url_prefix);
    if (config.no_sign_request)
        creation_args += ", 'NOSIGN'";
    else if (config.access_key_id.has_value() && config.secret_access_key.has_value())
        creation_args += fmt::format(", '{}', '{}'", config.access_key_id.value(), config.secret_access_key.value());

    const String query = fmt::format("CREATE DATABASE {} ENGINE = Monitor({})", backQuoteIfNeed(getDatabaseName()), creation_args);
    ASTPtr ast = parseQuery(parser, query.data(), query.data() + query.size(), "", 0, settings.max_parser_depth, settings.max_parser_backtracks);

    if (const auto database_comment = getDatabaseComment(); !database_comment.empty())
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.set(ast_create_query.comment, std::make_shared<ASTLiteral>(database_comment));
    }

    return ast;
}

/// It returns create table statement (even if table is detached)
ASTPtr DatabaseMonitor::getCreateTableQueryImpl(const String & table_name, ContextPtr, bool throw_on_error) const
{
    ASTPtr ast;
    StoragePtr storage = tryGetTable(table_name, getContext());
    bool has_table = storage != nullptr;
    bool is_system_storage = false;
    if (has_table)
        is_system_storage = storage->isSystemStorage();
    auto table_metadata_path = getObjectMetadataPath(table_name);
    try
    {
        ast = getCreateQueryFromMetadata(table_metadata_path, throw_on_error);
    }
    catch (const Exception & e)
    {
        if (!has_table && e.code() == ErrorCodes::FILE_DOESNT_EXIST && throw_on_error)
            throw Exception(ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY, "Table {} doesn't exist", backQuote(table_name));
        else if (!is_system_storage && throw_on_error)
            throw;
    }
    if (!ast && is_system_storage)
        ast = getCreateQueryFromStorage(table_name, storage, throw_on_error);
    return ast;
}

ASTPtr DatabaseMonitor::getCreateQueryFromMetadata(const String & database_metadata_path, bool throw_on_error) const
{
    ASTPtr ast = parseQueryFromMetadata(log, getContext(), database_metadata_path, throw_on_error);

    if (ast)
    {
        auto & ast_create_query = ast->as<ASTCreateQuery &>();
        ast_create_query.attach = false;
        ast_create_query.setDatabase(getDatabaseName());
    }

    return ast;
}

ASTPtr DatabaseMonitor::getCreateQueryFromStorage(const String & table_name, const StoragePtr & storage, bool throw_on_error) const
{
    auto metadata_ptr = storage->getInMemoryMetadataPtr();
    if (metadata_ptr == nullptr)
    {
        if (throw_on_error)
            throw Exception(ErrorCodes::CANNOT_GET_CREATE_TABLE_QUERY, "Cannot get metadata of {}.{}",
                            backQuote(getDatabaseName()), backQuote(table_name));
        else
            return nullptr;
    }

    /// setup create table query storage info.
    auto ast_engine = std::make_shared<ASTFunction>();
    ast_engine->name = storage->getName();
    ast_engine->no_empty_args = true;
    auto ast_storage = std::make_shared<ASTStorage>();
    ast_storage->set(ast_storage->engine, ast_engine);

    const Settings & settings = getContext()->getSettingsRef();
    auto create_table_query = DB::getCreateQueryFromStorage(
        storage,
        ast_storage,
        false,
        static_cast<unsigned>(settings.max_parser_depth),
        static_cast<unsigned>(settings.max_parser_backtracks),
        throw_on_error);

    create_table_query->set(create_table_query->as<ASTCreateQuery>()->comment,
                            std::make_shared<ASTLiteral>(storage->getInMemoryMetadata().comment));

    return create_table_query;
}

void DatabaseMonitor::createTable(
    ContextPtr local_context,
    const String & table_name,
    const StoragePtr & table,
    const ASTPtr & query)
{
    // const auto & settings = local_context->getSettingsRef();
    const auto & create = query->as<ASTCreateQuery &>();
    assert(table_name == create.getTable());

    /// Create a file with metadata if necessary - if the query is not ATTACH.
    /// Write the query of `ATTACH table` to it.

    /** The code is based on the assumption that all threads share the same order of operations
      * - creating the .sql.tmp file;
      * - adding a table to `tables`;
      * - rename .sql.tmp to .sql.
      */

    /// A race condition would be possible if a table with the same name is simultaneously created using CREATE and using ATTACH.
    /// But there is protection from it - see using DDLGuard in InterpreterCreateQuery.

    if (isTableExist(table_name, getContext()))
        throw Exception(
            ErrorCodes::TABLE_ALREADY_EXISTS, "Table {}.{} already exists", backQuote(getDatabaseName()), backQuote(table_name));

    waitDatabaseStarted();

    String table_metadata_path = getObjectMetadataPath(table_name);

    if (!create.attach)
        checkMetadataFilenameAvailability(table_name);

//    if (create.attach && fs::exists(table_metadata_path))
//    {
//        ASTPtr ast_detached = parseQueryFromMetadata(log, local_context, table_metadata_path);
//        auto & create_detached = ast_detached->as<ASTCreateQuery &>();
//
//        // either both should be Nil, either values should be equal
//        if (create.uuid != create_detached.uuid)
//            throw Exception(
//                ErrorCodes::TABLE_ALREADY_EXISTS,
//                "Table {}.{} already exist (detached or detached permanently). To attach it back "
//                "you need to use short ATTACH syntax (ATTACH TABLE {}.{};)",
//                backQuote(getDatabaseName()), backQuote(table_name),
//                backQuote(getDatabaseName()), backQuote(table_name));
//    }

    String statement;

    {
        statement = getObjectDefinitionFromCreateQuery(query);
        std::cout << statement << std::endl;

//        S3Settings s3_settings;
//        S3::URI s3_uri(fs::path(config.url_prefix));
//
//        std::shared_ptr<S3::Client> client = makeS3Client(
//            s3_uri,
//            config.access_key_id.value(),
//            config.secret_access_key.value(),
//            s3_settings,
//            getContext());
//        BlobStorageLogWriterPtr blob_storage_log;
        WriteBufferFromS3 wb(
            client,
            s3_uri.bucket,
            table_metadata_path,
            DBMS_DEFAULT_BUFFER_SIZE,
            s3_settings.request_settings,
            blob_storage_log);
        writeString(statement, wb);
        wb.finalize();
    }

    /// Add a table to the map of known tables.
//     attachTable(local_context, create.getTable(), table, getTableDataPath(create));
//     table_name_to_path.emplace(std::make_pair(create.getTable(), getTableDataPath(create)));

    commitCreateTable(create, table, "", table_metadata_path, local_context);
}

void DatabaseMonitor::commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                                       const String & /*table_metadata_tmp_path*/, const String & /*table_metadata_path*/,
                                       ContextPtr /*query_context*/)
{
    auto table_data_path = getTableDataPath(query);
    try
    {
        std::lock_guard lock{mutex};

        chassert(DatabaseCatalog::instance().hasUUIDMapping(query.uuid));

        attachTableUnlocked(query.getTable(), table);   /// Should never throw
        table_name_to_path.emplace(query.getTable(), table_data_path);
    }
    catch (...)
    {
        throw;
    }

}

void DatabaseMonitor::attachTable(ContextPtr /* context_ */, const String & name, const StoragePtr & table, const String & relative_table_path)
{
    assert(relative_table_path != data_path && !relative_table_path.empty());
    auto table_id = table->getStorageID();
    try
    {
        std::lock_guard lock{mutex};
        attachTableUnlocked(name, table);
        table_name_to_path.emplace(std::make_pair(name, relative_table_path));
    }
    catch (...)
    {
        throw;
    }
}

void DatabaseMonitor::dropTable(ContextPtr local_context, const String & table_name, bool /*sync*/)
{
    waitDatabaseStarted();

    String table_metadata_path = getObjectMetadataPath(table_name);
    String table_data_path_relative = getTableDataPath(table_name);

    if (table_data_path_relative.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Path is empty");

    detachTable(local_context, table_name);
//    StoragePtr table = detachTable(local_context, table_name);
//
//    bool renamed = false;
//    try
//    {
//        fs::rename(table_metadata_path, table_metadata_path_drop);
//        renamed = true;
//        // The table might be not loaded for Lazy database engine.
//        if (table)
//        {
//            table->drop();
//            table->is_dropped = true;
//        }
//    }
//    catch (...)
//    {
//        LOG_WARNING(log, getCurrentExceptionMessageAndPattern(/* with_stacktrace */ true));
//        if (table)
//            attachTable(local_context, table_name, table, table_data_path_relative);
//        if (renamed)
//            fs::rename(table_metadata_path_drop, table_metadata_path);
//        throw;
//    }
//
    for (const auto & [disk_name, disk] : getContext()->getDisksMap())
    {
        if (disk->isReadOnly() || !disk->exists(table_data_path_relative))
            continue;

        LOG_INFO(log, "Removing data directory from disk {} with path {} for dropped table {} ", disk_name, table_data_path_relative, table_name);
        disk->removeRecursive(table_data_path_relative);
    }
//    S3Settings s3_settings;
//    S3::URI s3_uri(fs::path(config.url_prefix));
//
//    std::shared_ptr<S3::Client> client = makeS3Client(
//        s3_uri,
//        config.access_key_id.value(),
//        config.secret_access_key.value(),
//        s3_settings,
//        getContext());
    S3::DeleteObjectRequest req;
    req.SetBucket(s3_uri.bucket);
    req.SetKey(getMetadataPath().substr(1)+table_name+".sql");
    client->DeleteObject(req);
//    (void)fs::remove(table_metadata_path_drop);
}

String DatabaseMonitor::getObjectMetadataPath(const String & object_name) const
{
    return getMetadataPath() + escapeForFileName(object_name) + ".sql";
}

/// from s3 key
ASTPtr DatabaseMonitor::parseQueryFromMetadata(
    LoggerPtr logger,
    ContextPtr local_context,
    const String & metadata_file_path,
    bool throw_on_error /*= true*/,
    bool remove_empty /*= false*/) const
{
    String query;

//    S3Settings s3_settings;
//    S3::URI s3_uri(fs::path(config.url_prefix));
//    std::shared_ptr<S3::Client> client = makeS3Client(
//        s3_uri,
//        config.access_key_id.value(),
//        config.secret_access_key.value(),
//        s3_settings,
//        getContext());
    /// metadata_file_path means file_name
    ReadBufferFromS3 in(
        client, s3_uri.bucket,
        fs::path(s3_uri.key) / metadata_file_path,
        s3_uri.version_id,
        s3_settings.request_settings,
        local_context->getReadSettings());

//    int metadata_file_fd = ::open(metadata_file_path.c_str(), O_RDONLY | O_CLOEXEC);
//
//    if (metadata_file_fd == -1)
//    {
//        if (errno == ENOENT && !throw_on_error)
//            return nullptr;
//
//        ErrnoException::throwFromPath(
//            errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE,
//            metadata_file_path,
//            "Cannot open file {}",
//            metadata_file_path);
//    }
//
//    ReadBufferFromFile in(metadata_file_fd, metadata_file_path, METADATA_FILE_BUFFER_SIZE);
    readStringUntilEOF(query, in);

    /** Empty files with metadata are generated after a rough restart of the server.
      * Remove these files to slightly reduce the work of the admins on startup.
      */
    if (remove_empty && query.empty())
    {
        if (logger)
            LOG_ERROR(logger, "File {} is empty. Removing.", metadata_file_path);
        // (void)fs::remove(metadata_file_path);
        return nullptr;
    }

    auto settings = local_context->getSettingsRef();
    ParserCreateQuery parser;
    const char * pos = query.data();
    std::string error_message;
    auto ast = tryParseQuery(parser, pos, pos + query.size(), error_message, /* hilite = */ false,
                             "in file " + metadata_file_path, /* allow_multi_statements = */ false, 0, settings.max_parser_depth, settings.max_parser_backtracks, true);

    if (!ast && throw_on_error)
        throw Exception::createDeprecated(error_message, ErrorCodes::SYNTAX_ERROR);
    else if (!ast)
        return nullptr;

    auto & create = ast->as<ASTCreateQuery &>();
    if (create.table && create.uuid != UUIDHelpers::Nil)
    {
        String table_name = unescapeForFileName(fs::path(metadata_file_path).stem());

        if (create.getTable() != TABLE_WITH_UUID_NAME_PLACEHOLDER && logger)
            LOG_WARNING(
                logger,
                "File {} contains both UUID and table name. Will use name `{}` instead of `{}`",
                metadata_file_path,
                table_name,
                create.getTable());
        create.setTable(table_name);
    }

    return ast;
}

void DatabaseMonitor::iterateMetadataFiles(ContextPtr local_context, const IteratingFunction & process_metadata_file) const
{
    auto process_tmp_drop_metadata_file = [&](const String & file_name)
    {
        assert(getUUID() == UUIDHelpers::Nil);
        static const char * tmp_drop_ext = ".sql.tmp_drop";
        const std::string object_name = file_name.substr(0, file_name.size() - strlen(tmp_drop_ext));

        if (fs::exists(local_context->getPath() + getDataPath() + '/' + object_name))
        {
            fs::rename(getMetadataPath() + file_name, getMetadataPath() + object_name + ".sql");
            LOG_WARNING(log, "Object {} was not dropped previously and will be restored", backQuote(object_name));
            process_metadata_file(object_name + ".sql");
        }
        else
        {
            LOG_INFO(log, "Removing file {}", getMetadataPath() + file_name);
            (void)fs::remove(getMetadataPath() + file_name);
        }
    };

    /// Metadata files to load: name and flag for .tmp_drop files
    std::vector<std::pair<String, bool>> metadata_files;


    //todo: need iterate on cos

//    S3Settings s3_settings;
//    S3::URI s3_uri(fs::path(config.url_prefix));
//    std::shared_ptr<S3::Client> client = makeS3Client(
//        s3_uri,
//        config.access_key_id.value(),
//        config.secret_access_key.value(),
//        s3_settings,
//        getContext());
    S3::ListObjectsV2Request req;
    req.SetBucket(s3_uri.bucket);
    req.SetPrefix(getMetadataPath().substr(1));
    auto outcome = client->ListObjectsV2(req);
    for (const auto &object : outcome.GetResult().GetContents()) {
        auto remote_path = fs::path(object.GetKey());
        if (endsWith(remote_path, ".sql")) {
            metadata_files.emplace_back(remote_path.filename(), true);
        }
    }

//    fs::directory_iterator dir_end;
//
//    for (fs::directory_iterator dir_it(getMetadataPath()); dir_it != dir_end; ++dir_it)
//    {
//        String file_name = dir_it->path().filename();
//        /// For '.svn', '.gitignore' directory and similar.
//        if (file_name.at(0) == '.')
//            continue;
//
//        /// There are .sql.bak files - skip them.
//        if (endsWith(file_name, ".sql.bak"))
//            continue;
//
//        /// Permanently detached table flag
//        if (endsWith(file_name, ".sql.detached"))
//            continue;
//
//        if (endsWith(file_name, ".sql.tmp_drop"))
//        {
//            /// There are files that we tried to delete previously
//            metadata_files.emplace_back(file_name, false);
//        }
//        else if (endsWith(file_name, ".sql.tmp"))
//        {
//            /// There are files .sql.tmp - delete
//            LOG_INFO(log, "Removing file {}", dir_it->path().string());
//            (void)fs::remove(dir_it->path());
//        }
//        else if (endsWith(file_name, ".sql"))
//        {
//            /// The required files have names like `table_name.sql`
//            metadata_files.emplace_back(file_name, true);
//        }
//        else
//            throw Exception(ErrorCodes::INCORRECT_FILE_NAME, "Incorrect file extension: {} in metadata directory {}", file_name, getMetadataPath());
//    }

    std::sort(metadata_files.begin(), metadata_files.end());
    metadata_files.erase(std::unique(metadata_files.begin(), metadata_files.end()), metadata_files.end());


    /// Read and parse metadata in parallel
    ThreadPool pool(CurrentMetrics::DatabaseOnDiskThreads, CurrentMetrics::DatabaseOnDiskThreadsActive, CurrentMetrics::DatabaseOnDiskThreadsScheduled);
    const auto batch_size = metadata_files.size() / pool.getMaxThreads() + 1;
    for (auto it = metadata_files.begin(); it < metadata_files.end(); std::advance(it, batch_size))
    {
        std::span batch{it, std::min(std::next(it, batch_size), metadata_files.end())};
        pool.scheduleOrThrow(
            [batch, &process_metadata_file, &process_tmp_drop_metadata_file]() mutable
            {
                setThreadName("DatabaseMonitor");
                for (const auto & file : batch)
                    if (file.second)
                        process_metadata_file(file.first);
                    else
                        process_tmp_drop_metadata_file(file.first);
            }, Priority{}, getContext()->getSettingsRef().lock_acquire_timeout.totalMicroseconds());
    }
    pool.wait();
}

void DatabaseMonitor::loadTablesMetadata(ContextPtr local_context, ParsedTablesMetadata & metadata, bool is_startup)
{
    size_t prev_tables_count = metadata.parsed_tables.size();
    size_t prev_total_dictionaries = metadata.total_dictionaries;

    auto process_metadata = [&metadata, is_startup, this](const String & file_name)
    {
        fs::path path(getMetadataPath());
        fs::path file_path(file_name);
        fs::path full_path = path / file_path;

        try
        {
            auto ast = parseQueryFromMetadata(log, getContext(), full_path.string(), /*throw_on_error*/ true, /*remove_empty*/ false);
            if (ast)
            {
                FunctionNameNormalizer::visit(ast.get());
                auto * create_query = ast->as<ASTCreateQuery>();
                /// NOTE No concurrent writes are possible during database loading
                create_query->setDatabase(TSA_SUPPRESS_WARNING_FOR_READ(database_name));

                /// Even if we don't load the table we can still mark the uuid of it as taken.
                if (create_query->uuid != UUIDHelpers::Nil)
                {
                    /// A bit tricky way to distinguish ATTACH DATABASE and server startup (actually it's "force_attach" flag).
                    if (is_startup)
                    {
                        /// Server is starting up. Lock UUID used by permanently detached table.
                        DatabaseCatalog::instance().addUUIDMapping(create_query->uuid);
                    }
                    else if (!DatabaseCatalog::instance().hasUUIDMapping(create_query->uuid))
                    {
                        /// It's ATTACH DATABASE. UUID for permanently detached table must be already locked.
                        /// FIXME MaterializedPostgreSQL works with UUIDs incorrectly and breaks invariants
                        if (getEngineName() != "MaterializedPostgreSQL")
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot find UUID mapping for {}, it's a bug", create_query->uuid);
                    }
                }


                QualifiedTableName qualified_name{TSA_SUPPRESS_WARNING_FOR_READ(database_name), create_query->getTable()};

                std::lock_guard lock{metadata.mutex};
                metadata.parsed_tables[qualified_name] = ParsedTableMetadata{full_path.string(), ast};
                metadata.total_dictionaries += create_query->is_dictionary;
            }
        }
        catch (Exception & e)
        {
            e.addMessage("Cannot parse definition from metadata file " + full_path.string());
            throw;
        }
    };

    iterateMetadataFiles(local_context, process_metadata);

    size_t objects_in_database = metadata.parsed_tables.size() - prev_tables_count;
    size_t dictionaries_in_database = metadata.total_dictionaries - prev_total_dictionaries;
    size_t tables_in_database = objects_in_database - dictionaries_in_database;

    LOG_INFO(log, "Metadata processed, database {} has {} tables and {} dictionaries in total.",
             TSA_SUPPRESS_WARNING_FOR_READ(database_name), tables_in_database, dictionaries_in_database);
}

void DatabaseMonitor::loadTableFromMetadata(
    ContextMutablePtr local_context,
    const String & file_path,
    const QualifiedTableName & name,
    const ASTPtr & ast,
    LoadingStrictnessLevel mode)
{
    assert(name.database == TSA_SUPPRESS_WARNING_FOR_READ(database_name));
    const auto & query = ast->as<const ASTCreateQuery &>();

    LOG_TRACE(log, "Loading table {}", name.getFullName());

    try
    {
        auto [table_name, table] = createTableFromAST(
            query,
            name.database,
            getTableDataPath(query),
            local_context,
            mode);

        attachTable(local_context, table_name, table, getTableDataPath(query));
    }
    catch (Exception & e)
    {
        e.addMessage(
            "Cannot attach table " + backQuote(name.database) + "." + backQuote(query.getTable()) + " from metadata file " + file_path
            + " from query " + serializeAST(query));
        throw;
    }
}

LoadTaskPtr DatabaseMonitor::loadTableFromMetadataAsync(
    AsyncLoader & async_loader,
    LoadJobSet load_after,
    ContextMutablePtr local_context,
    const String & file_path,
    const QualifiedTableName & name,
    const ASTPtr & ast,
    LoadingStrictnessLevel mode)
{
    std::scoped_lock lock(mutex);
    auto job = makeLoadJob(
        std::move(load_after),
        TablesLoaderBackgroundLoadPoolId,
        fmt::format("load table {}", name.getFullName()),
        [this, local_context, file_path, name, ast, mode] (AsyncLoader &, const LoadJobPtr &)
        {
            loadTableFromMetadata(local_context, file_path, name, ast, mode);
        });

    return load_table[name.table] = makeLoadTask(async_loader, {job});
}

LoadTaskPtr DatabaseMonitor::startupTableAsync(
    AsyncLoader & async_loader,
    LoadJobSet startup_after,
    const QualifiedTableName & name,
    LoadingStrictnessLevel /*mode*/)
{
    std::scoped_lock lock(mutex);

    /// Initialize progress indication on the first call
    if (total_tables_to_startup == 0)
    {
        total_tables_to_startup = tables.size();
        startup_watch.restart();
    }

    auto job = makeLoadJob(
        std::move(startup_after),
        TablesLoaderBackgroundStartupPoolId,
        fmt::format("startup table {}", name.getFullName()),
        [this, name] (AsyncLoader &, const LoadJobPtr &)
        {
            if (auto table = tryGetTableNoWait(name.table))
            {
                /// Since startup() method can use physical paths on disk we don't allow any exclusive actions (rename, drop so on)
                /// until startup finished.
                auto table_lock_holder = table->lockForShare(RWLockImpl::NO_QUERY, getContext()->getSettingsRef().lock_acquire_timeout);
                table->startup();

                /// If table is ReplicatedMergeTree after conversion from MergeTree,
                /// it is in readonly mode due to metadata in zookeeper missing.
                // restoreMetadataAfterConvertingToReplicated(table, name);

                logAboutProgress(log, ++tables_started, total_tables_to_startup, startup_watch);
            }
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {}.{} doesn't exist during startup",
                                backQuote(name.database), backQuote(name.table));
        });

    return startup_table[name.table] = makeLoadTask(async_loader, {job});
}

LoadTaskPtr DatabaseMonitor::startupDatabaseAsync(
    AsyncLoader & async_loader,
    LoadJobSet startup_after,
    LoadingStrictnessLevel /*mode*/)
{
    auto job = makeLoadJob(
        std::move(startup_after),
        TablesLoaderBackgroundStartupPoolId,
        fmt::format("startup Monitor database {}", getDatabaseName()),
        ignoreDependencyFailure,
        [] (AsyncLoader &, const LoadJobPtr &)
        {
            // NOTE: this job is no-op, but it is required for correct dependency handling
            // 1) startup should be done after tables loading
            // 2) load or startup errors for tables should not lead to not starting up the whole database
        });
    std::scoped_lock lock(mutex);
    return startup_database_task = makeLoadTask(async_loader, {job});
}

void registerDatabaseMonitor(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        auto * engine_define = args.create_query.storage;
        const ASTFunction * engine = engine_define->engine;

        DatabaseMonitor::Configuration config;

        if (engine->arguments && !engine->arguments->children.empty())
        {
            ASTs & engine_args = engine->arguments->children;
            config = DatabaseMonitor::parseArguments(engine_args, args.context);
        }

        return std::make_shared<DatabaseMonitor>(args.database_name,
                                                 args.metadata_path,
                                                 args.uuid,
                                                 config,
                                                 args.context);
    };
    factory.registerDatabase("Monitor", create_fn);
}
}

