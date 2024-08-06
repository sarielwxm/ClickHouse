#include <Storages/StorageTSearchMonitorMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MutationCommands.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Kafka/KafkaConsumer.h>
#include <Storages/Kafka/KafkaSettings.h>
#include <Common/SettingsChanges.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/NamedCollectionsHelpers.h>

#include <Common/config_version.h>

#include <Storages/Kafka/KafkaProducer.h>
#include <Storages/Kafka/KafkaSource.h>
#include <Storages/MessageQueueSink.h>

#include <cppkafka/configuration.h>


namespace DB
{

static const std::unordered_set<std::string_view> optional_configuration_keys = {
    "kafka_broker_list",
    "kafka_topic_list",
    "kafka_group_name",
    "kafka_format",
};

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

const String CONFIG_KAFKA_TAG = "kafka";
const String CONFIG_KAFKA_TOPIC_TAG = "kafka_topic";
const String CONFIG_KAFKA_CONSUMER_TAG = "consumer";
const String CONFIG_KAFKA_PRODUCER_TAG = "producer";
const String CONFIG_NAME_TAG = "name";

StorageTSearchMonitorMergeTree::StorageTSearchMonitorMergeTree(
    const StorageID & table_id_,
    const String & relative_data_path_,
    const StorageInMemoryMetadata & metadata,
    LoadingStrictnessLevel mode,
    ContextMutablePtr context_,
    const String & date_column_name,
    const MergingParams & merging_params_,
    const Configuration& config_,
    std::unique_ptr<MergeTreeSettings> settings_)
    : StorageMergeTree(
          table_id_,
          relative_data_path_,
          metadata,
          mode,
          context_,
          date_column_name,
          merging_params_,
          std::move(settings_))
    , merger_mutator(*this)
    , config(config_)
{
    // merger_mutator.merges_blocker.cancel();
}

void StorageTSearchMonitorMergeTree::startup()
{
    StorageMergeTree::startup();
    cppkafka::Configuration conf;
    conf.set("metadata.broker.list", config.kafka_broker_list);
    const Settings & settings = getContext()->getSettingsRef();
    size_t poll_timeout = settings.stream_poll_timeout_ms.totalMilliseconds();
    const auto & header = getInMemoryMetadataPtr()->getSampleBlockNonMaterialized();
    producer = std::make_unique<KafkaProducer>(
        std::make_shared<cppkafka::Producer>(conf), config.kafka_topic_list, std::chrono::milliseconds(poll_timeout), shutdown_called, header);
}

void StorageTSearchMonitorMergeTree::shutdown(bool)
{
    shutdown_called = true;
    if (producer)
        producer->finish();
    StorageMergeTree::shutdown(false);
}

StorageTSearchMonitorMergeTree::Configuration StorageTSearchMonitorMergeTree::parseArguments(ASTs engine_args, ContextPtr context_)
{
    Configuration result;

    const std::string supported_signature =
        " - TSearchMonitorMergeTree('url', 'url', 'url', 'url', 'url')\n";
    const auto error_message =
        fmt::format("Engine TSearchMonitorMergeTree must have the following arguments signature\n{}", supported_signature);

    for (auto & arg : engine_args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context_);

    if (engine_args.size() > 5)
        throw Exception::createRuntime(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, error_message.c_str());

    if (engine_args.empty())
        return result;

    result.kafka_broker_list = checkAndGetLiteralArgument<String>(engine_args[0], "kafka_broker_list");
    result.kafka_topic_list = checkAndGetLiteralArgument<String>(engine_args[1], "kafka_topic_list");
    result.kafka_topic_partition = checkAndGetLiteralArgument<UInt64>(engine_args[2], "kafka_topic_partition");
    result.kafka_group_name = checkAndGetLiteralArgument<String>(engine_args[3], "kafka_group_name");
    result.kafka_format = checkAndGetLiteralArgument<String>(engine_args[4], "kafka_format");

//    // url, NOSIGN
//    if (engine_args.size() == 2)
//    {
//        auto second_arg = checkAndGetLiteralArgument<String>(engine_args[1], "NOSIGN");
//        if (boost::iequals(second_arg, "NOSIGN"))
//            result.no_sign_request = true;
//        else
//            throw Exception::createRuntime(ErrorCodes::BAD_ARGUMENTS, error_message.c_str());
//    }
//
//    // url, access_key_id, secret_access_key
//    if (engine_args.size() == 3)
//    {
//        auto key_id = checkAndGetLiteralArgument<String>(engine_args[1], "access_key_id");
//        auto secret_key = checkAndGetLiteralArgument<String>(engine_args[2], "secret_access_key");
//
//        if (key_id.empty() || secret_key.empty() || boost::iequals(key_id, "NOSIGN"))
//            throw Exception::createRuntime(ErrorCodes::BAD_ARGUMENTS, error_message.c_str());
//
//        result.access_key_id = key_id;
//        result.secret_access_key = secret_key;
//    }

    return result;
}

void StorageTSearchMonitorMergeTree::produceMessageToKafka(const String & message, size_t rows_in_message, size_t last_row)
{
    producer->start(getContext());
    producer->produce(message, rows_in_message, {}, last_row, config.kafka_topic_partition);
    producer->finish();
}

//cppkafka::Configuration StorageTSearchMonitorMergeTree::getProducerConfiguration()
//{
//    cppkafka::Configuration conf;
//    conf.set("metadata.broker.list", brokers);
//    conf.set("client.id", client_id);
//    conf.set("client.software.name", VERSION_NAME);
//    conf.set("client.software.version", VERSION_DESCRIBE);
//
//    updateGlobalConfiguration(conf);
//    updateProducerConfiguration(conf);
//
//    for (auto & property : conf.get_all())
//    {
//        LOG_TRACE(log, "Producer set property {}:{}", property.first, property.second);
//    }
//
//    return conf;
//}
//
//void StorageTSearchMonitorMergeTree::updateGlobalConfiguration(cppkafka::Configuration & kafka_config)
//{
//    const auto & config = getContext()->getConfigRef();
//    loadFromConfig(kafka_config, config, collection_name, CONFIG_KAFKA_TAG, topics);
//
//#if USE_KRB5
//    if (kafka_config.has_property("sasl.kerberos.kinit.cmd"))
//        LOG_WARNING(log, "sasl.kerberos.kinit.cmd configuration parameter is ignored.");
//
//    kafka_config.set("sasl.kerberos.kinit.cmd","");
//    kafka_config.set("sasl.kerberos.min.time.before.relogin","0");
//
//    if (kafka_config.has_property("sasl.kerberos.keytab") && kafka_config.has_property("sasl.kerberos.principal"))
//    {
//        String keytab = kafka_config.get("sasl.kerberos.keytab");
//        String principal = kafka_config.get("sasl.kerberos.principal");
//        LOG_DEBUG(log, "Running KerberosInit");
//        try
//        {
//            kerberosInit(keytab,principal);
//        }
//        catch (const Exception & e)
//        {
//            LOG_ERROR(log, "KerberosInit failure: {}", getExceptionMessage(e, false));
//        }
//        LOG_DEBUG(log, "Finished KerberosInit");
//    }
//#else // USE_KRB5
//    if (kafka_config.has_property("sasl.kerberos.keytab") || kafka_config.has_property("sasl.kerberos.principal"))
//        LOG_WARNING(log, "Ignoring Kerberos-related parameters because ClickHouse was built without krb5 library support.");
//#endif // USE_KRB5
//
//    // No need to add any prefix, messages can be distinguished
//    kafka_config.set_log_callback(
//        [this](cppkafka::KafkaHandleBase & handle, int level, const std::string & facility, const std::string & message)
//        {
//            auto [poco_level, client_logs_level] = parseSyslogLevel(level);
//            const auto & kafka_object_config = handle.get_configuration();
//            const std::string client_id_key{"client.id"};
//            chassert(kafka_object_config.has_property(client_id_key) && "Kafka configuration doesn't have expected client.id set");
//            LOG_IMPL(
//                log,
//                client_logs_level,
//                poco_level,
//                "[client.id:{}] [rdk:{}] {}",
//                kafka_object_config.get(client_id_key),
//                facility,
//                message);
//        });
//
//    /// NOTE: statistics should be consumed, otherwise it creates too much
//    /// entries in the queue, that leads to memory leak and slow shutdown.
//    if (!kafka_config.has_property("statistics.interval.ms"))
//    {
//        // every 3 seconds by default. set to 0 to disable.
//        kafka_config.set("statistics.interval.ms", "3000");
//    }
//
//    // Configure interceptor to change thread name
//    //
//    // TODO: add interceptors support into the cppkafka.
//    // XXX:  rdkafka uses pthread_set_name_np(), but glibc-compatibliity overrides it to noop.
//    {
//        // This should be safe, since we wait the rdkafka object anyway.
//        void * self = static_cast<void *>(this);
//
//        int status;
//
//        status = rd_kafka_conf_interceptor_add_on_new(kafka_config.get_handle(),
//                                                      "init", StorageKafkaInterceptors::rdKafkaOnNew, self);
//        if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
//            LOG_ERROR(log, "Cannot set new interceptor due to {} error", status);
//
//        // cppkafka always copy the configuration
//        status = rd_kafka_conf_interceptor_add_on_conf_dup(kafka_config.get_handle(),
//                                                           "init", StorageKafkaInterceptors::rdKafkaOnConfDup, self);
//        if (status != RD_KAFKA_RESP_ERR_NO_ERROR)
//            LOG_ERROR(log, "Cannot set dup conf interceptor due to {} error", status);
//    }
//}
//
//void StorageTSearchMonitorMergeTree::updateProducerConfiguration(cppkafka::Configuration & kafka_config)
//{
//    const auto & config = getContext()->getConfigRef();
//    loadProducerConfig(kafka_config, config, collection_name, CONFIG_KAFKA_TAG, topics);
//}

}

