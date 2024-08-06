#pragma once

#include <Databases/DatabasesCommon.h>
#include <Common/escapeForFileName.h>
#include <Parsers/ASTCreateQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/Kafka/KafkaConsumer.h>
#include <IO/S3/Client.h>
#include <IO/S3Settings.h>
#include <IO/WriteBufferFromS3.h>
#include <Disks/ObjectStorages/S3/S3Capabilities.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/MultiVersion.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Core/BackgroundSchedulePool.h>

namespace DB
{

using KafkaConsumerPtr = std::shared_ptr<KafkaConsumer>;
using ConsumerPtr = std::shared_ptr<cppkafka::Consumer>;

class DatabaseMonitor : public DatabaseWithOwnTablesBase
{
public:
    struct Configuration
    {
        std::string url_prefix;

        bool no_sign_request = false;

        std::optional<std::string> access_key_id;
        std::optional<std::string> secret_access_key;

        std::string remote_database_name;
    };

    DatabaseMonitor(
        const String & name, const String & metadata_path_, const String & data_path_, UUID uuid,
        const Configuration& config, const String & logger, ContextPtr local_context);

    DatabaseMonitor(const String & name, const String & metadata_path_, UUID uuid, const Configuration& config, ContextPtr local_context);
    ~DatabaseMonitor() override
    {
        shutdown_called = true;
        stopped = true;
        if (getContext()->getConfigRef().getBool("monitor", false))
            task->deactivate();
        if (kafka_consumer)
        {
            kafka_consumer->unsubscribe();
        }
    }

    String getEngineName() const override { return "Monitor"; }

    UUID getUUID() const override { return db_uuid; }

    static Configuration parseArguments(ASTs engine_args, ContextPtr context);

    void createTable(
        ContextPtr context,
        const String & table_name,
        const StoragePtr & table,
        const ASTPtr & query) override;

    void dropTable(
        ContextPtr context,
        const String & table_name,
        bool sync) override;

    void attachTable(ContextPtr context, const String & name, const StoragePtr & table, const String & relative_table_path) override;

    ASTPtr getCreateDatabaseQuery() const override;
    ASTPtr getCreateTableQueryImpl(
        const String & table_name,
        ContextPtr context,
        bool throw_on_error) const override;

    ASTPtr parseQueryFromMetadata(LoggerPtr log, ContextPtr context, const String & metadata_file_path, bool throw_on_error = true, bool remove_empty = false) const;

    String getObjectMetadataPath(const String & object_name) const override;

    /// DatabaseMonitor allows to create tables, which store data on s3.
    String getDataPath() const override { return data_path; }
    String getTableDataPath(const String & table_name) const override {
        auto it = table_name_to_path.find(table_name);
        if (it == table_name_to_path.end())
            throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table {} not found in database monitor", table_name);
        assert(it->second != data_path && !it->second.empty());
        return it->second;
        // return data_path + escapeForFileName(table_name) + "/";
    }
    String getTableDataPath(const ASTCreateQuery & query) const override {
        auto tmp = data_path + DatabaseCatalog::getPathForUUID(query.uuid);
        //return getTableDataPath(query.getTable());
        return tmp;
    }
    String getMetadataPath() const override { return metadata_path; }

    bool supportsLoadingInTopologicalOrder() const override { return true; }

//    bool isTableExist(const String & name, ContextPtr context) const override;

    bool checkUrl(const std::string & url, ContextPtr context_, bool throw_on_error) const;
    std::string getFullUrl(const std::string & name) const;

    void loadTablesMetadata(ContextPtr context, ParsedTablesMetadata & metadata, bool is_startup) override;

    void loadTableFromMetadata(
        ContextMutablePtr local_context,
        const String & file_path,
        const QualifiedTableName & name,
        const ASTPtr & ast,
        LoadingStrictnessLevel mode) override;

    LoadTaskPtr loadTableFromMetadataAsync(
        AsyncLoader & async_loader,
        LoadJobSet load_after,
        ContextMutablePtr local_context,
        const String & file_path,
        const QualifiedTableName & name,
        const ASTPtr & ast,
        LoadingStrictnessLevel mode) override;

    LoadTaskPtr startupTableAsync(
        AsyncLoader & async_loader,
        LoadJobSet startup_after,
        const QualifiedTableName & name,
        LoadingStrictnessLevel mode) override;

    LoadTaskPtr startupDatabaseAsync(AsyncLoader & async_loader, LoadJobSet startup_after, LoadingStrictnessLevel mode) override;

    const Configuration& getConfig() const { return config; }
protected:
    using IteratingFunction = std::function<void(const String &)>;
    void iterateMetadataFiles(ContextPtr context, const IteratingFunction & process_metadata_file) const;
    void commitCreateTable(const ASTCreateQuery & query, const StoragePtr & table,
                           const String & table_metadata_tmp_path, const String & table_metadata_path, ContextPtr query_context);

private:
    ASTPtr getCreateQueryFromMetadata(const String & metadata_path, bool throw_on_error) const;
    ASTPtr getCreateQueryFromStorage(const String & table_name, const StoragePtr & storage, bool throw_on_error) const;

    bool consumeKafkaMessages();
    void threadFunc();

    const Configuration config;
    mutable Tables loaded_tables TSA_GUARDED_BY(mutex);

    std::unordered_map<String, LoadTaskPtr> load_table TSA_GUARDED_BY(mutex);
    std::unordered_map<String, LoadTaskPtr> startup_table TSA_GUARDED_BY(mutex);
    LoadTaskPtr startup_database_task TSA_GUARDED_BY(mutex);
    std::atomic<size_t> total_tables_to_startup{0};
    std::atomic<size_t> tables_started{0};
    AtomicStopwatch startup_watch;

    using NameToPathMap = std::unordered_map<String, String>;
    NameToPathMap table_name_to_path;

    const String metadata_path;
    const String data_path;
    const UUID db_uuid;

    std::vector<KafkaConsumerPtr> consumers;

    KafkaConsumerPtr kafka_consumer;

    std::atomic<bool> stopped = false;
    std::atomic<bool> shutdown_called = false;
    UInt64 reschedule_processing_interval_ms = 1000;
    UInt64 polling_min_timeout_ms = 1000;
    UInt64 polling_backoff_ms = 1000;

    S3Settings s3_settings;
    std::shared_ptr<S3::Client> client;
    BlobStorageLogWriterPtr blob_storage_log;
    S3::URI s3_uri;

    BackgroundSchedulePool::TaskHolder task;

};


}
