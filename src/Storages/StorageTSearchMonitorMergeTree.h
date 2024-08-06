#pragma once

#include <Storages/StorageMergeTree.h>
#include <Storages/Kafka/KafkaConsumer.h>
#include <Storages/Kafka/KafkaProducer.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Common/SettingsChanges.h>

#include <cppkafka/cppkafka.h>

namespace DB
{


using KafkaProducerPtr = std::unique_ptr<KafkaProducer>;
using ProducerPtr = std::shared_ptr<cppkafka::Producer>;

class StorageTSearchMonitorMergeTree : public StorageMergeTree
{
public:
    struct Configuration
    {
        std::string kafka_broker_list;
        std::string kafka_topic_list;
        UInt64 kafka_topic_partition;
        std::string kafka_group_name;
        std::string kafka_format;
    };

    StorageTSearchMonitorMergeTree(
        const StorageID & table_id_,
        const String & relative_data_path_,
        const StorageInMemoryMetadata & metadata,
        LoadingStrictnessLevel mode,
        ContextMutablePtr context_,
        const String & date_column_name,
        const MergingParams & merging_params_,
        const Configuration& config,
        std::unique_ptr<MergeTreeSettings> settings_);

    void startup() override;
    void shutdown(bool is_drop) override;

    std::string getName() const override { return "TSearchMonitor" + merging_params.getModeName() + "MergeTree"; }

    static Configuration parseArguments(ASTs engine_args, ContextPtr context);

    const Configuration& getConfig() const { return config; }

    bool addPart(
        MutableDataPartPtr & part,
        Transaction & out_transaction,
        DataPartsLock & lock,
        DataPartsVector * out_covered_parts)
    {
        return addTempPart(part, out_transaction, lock, out_covered_parts);
    }

    void produceMessageToKafka(const String & message, size_t rows_in_message, size_t last_row);

private:
    MergeTreeDataMergerMutator merger_mutator;

    const Configuration config;

    const Names topics;
    const String brokers;
//    const int partition;
    const String group;
    const String client_id;
    const String format_name;

    KafkaProducerPtr producer;
    std::atomic<bool> shutdown_called = false;

    /// Returns full consumer related configuration, also the configuration
    /// contains global kafka properties.
    cppkafka::Configuration getConsumerConfiguration(size_t consumer_number);
    /// Returns full producer related configuration, also the configuration
    /// contains global kafka properties.
    cppkafka::Configuration getProducerConfiguration();

    // Load Kafka global configuration
    // https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md#global-configuration-properties
    void updateGlobalConfiguration(cppkafka::Configuration & kafka_config);
    // Load Kafka properties from consumer configuration
    // NOTE: librdkafka allow to set a consumer property to a producer and vice versa,
    //       but a warning will be generated e.g:
    //       "Configuration property session.timeout.ms is a consumer property and
    //        will be ignored by this producer instance"
    void updateConsumerConfiguration(cppkafka::Configuration & kafka_config);
    // Load Kafka properties from producer configuration
    void updateProducerConfiguration(cppkafka::Configuration & kafka_config);
};

}
