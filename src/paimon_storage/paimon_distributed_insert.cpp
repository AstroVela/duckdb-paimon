/*-------------------------------------------------------------------------
 *
 * paimon_distributed_insert.cpp
 *
 * Copyright (c) 2026, Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * IDENTIFICATION
 *	  src/paimon_storage/paimon_distributed_insert.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "paimon_distributed_insert.hpp"

#include "paimon_catalog.hpp"
#include "paimon_insert.hpp"

#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/crypto/md5.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/distributed_write.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "paimon/catalog/table.h"
#include "paimon/commit_context.h"
#include "paimon/commit_message.h"
#include "paimon/defs.h"
#include "paimon/file_store_commit.h"
#include "paimon/file_store_write.h"
#include "paimon/fs/file_system.h"
#include "paimon/fs/file_system_factory.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/record_batch.h"
#include "paimon/schema/schema.h"
#include "paimon/snapshot/snapshot_info.h"
#include "paimon/write_context.h"

#include <algorithm>
#include <atomic>
#include <exception>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

namespace duckdb {

namespace {

static constexpr uint32_t PAIMON_DISTRIBUTED_INSERT_PROTOCOL_VERSION = 1;
static constexpr const char *PAIMON_DISTRIBUTED_INSERT_OPERATOR = "insert";
static constexpr const char *PAIMON_DISTRIBUTED_INSERT_FRAGMENT_CODEC = "paimon.append-commit-fragment";
static constexpr const char *PAIMON_DISTRIBUTED_ATTEMPT_MANIFEST_ARTIFACT = "paimon_attempt_manifest";
static constexpr const char *PAIMON_DISTRIBUTED_ATTEMPT_MANIFEST_CODEC = "paimon.append-attempt-manifest";
static constexpr const char *PAIMON_DISTRIBUTED_ATTEMPT_MANIFEST_SUFFIX = ".commit";
static constexpr const char *PAIMON_DISTRIBUTED_OPERATION_DIRECTORY = "operations";
static constexpr const char *PAIMON_DISTRIBUTED_OPERATION_MARKER_SUFFIX = ".open";
static constexpr const char *PAIMON_DISTRIBUTED_RECOVERY_DIRECTORY = "recovery";
static constexpr const char *PAIMON_DISTRIBUTED_RECOVERY_INTENT_SUFFIX = ".intent";
static constexpr idx_t PAIMON_DISTRIBUTED_ATTEMPT_DIGEST_SIZE = 32;

enum class PaimonDistributedRecoveryAction : uint8_t { COMMIT = 1, ABORT = 2 };

struct PaimonDistributedInsertTransport {
	string operation_id;
	string database_name;
	string table_name;
	string table_uuid;
	string table_path;
	string table_schema_json;
	int64_t schema_id = -1;
	bool append_only = false;
	bool has_snapshot = false;
	int64_t snapshot_id = 0;
	int64_t commit_identifier = 0;
	vector<LogicalType> input_types;
	vector<string> input_names;
	vector<string> part_keys;
	string null_part_name;
	map<string, string> portable_options;
};

struct PaimonDistributedCommitEnvelope {
	string operation_id;
	string table_uuid;
	string table_path;
	string schema_fingerprint;
	int64_t schema_id = -1;
	bool has_snapshot = false;
	int64_t snapshot_id = 0;
	int64_t commit_identifier = 0;
	string query_id;
	string task_attempt_id;
	string writer_commit_user;
	int32_t commit_message_version = 0;
	idx_t message_count = 0;
	idx_t row_count = 0;
	string serialized_messages;
};

struct PaimonDistributedTargetState {
	string table_uuid;
	string table_path;
	string table_schema_json;
	int64_t schema_id = -1;
	bool append_only = false;
	bool has_snapshot = false;
	int64_t snapshot_id = 0;
	vector<string> field_names;
	vector<string> part_keys;
	string null_part_name;
};

using PaimonSelectedAttemptManifests = map<string, optional_ptr<const string>>;

struct PaimonDecodedDistributedCommit {
	std::vector<std::shared_ptr<paimon::CommitMessage>> messages;
	PaimonSelectedAttemptManifests selected_attempt_manifests;
	idx_t row_count = 0;
};

struct PaimonDistributedRecoveryIntent {
	PaimonDistributedInsertTransport transport;
	PaimonDistributedRecoveryAction action = PaimonDistributedRecoveryAction::ABORT;
	map<string, string> selected_attempt_manifests;
	idx_t row_count = 0;
};

static bool IsAttemptManifestName(const string &name);

static idx_t CheckedAdd(idx_t left, idx_t right, const char *description) {
	if (right > NumericLimits<idx_t>::Maximum() - left) {
		throw InvalidInputException("Distributed Paimon INSERT %s exceeds idx_t", description);
	}
	return left + right;
}

static void ValidateVaneTaskIdentityComponent(const string &component, const char *description) {
	if (component.empty() || (component.size() > 1 && component[0] == '0')) {
		throw InvalidInputException("Distributed Paimon INSERT has an invalid Vane %s", description);
	}
	for (const auto character : component) {
		if (!StringUtil::CharacterIsDigit(character)) {
			throw InvalidInputException("Distributed Paimon INSERT has an invalid Vane %s", description);
		}
	}
}

static string VaneLogicalTaskIdentity(const string &query_id, const string &task_attempt_id) {
	const auto prefix = query_id + ".";
	if (query_id.empty() || task_attempt_id.compare(0, prefix.size(), prefix) != 0) {
		throw InvalidInputException("Distributed Paimon INSERT task attempt does not match its Vane query identity");
	}
	const auto components = StringUtil::Split(task_attempt_id.substr(prefix.size()), '.');
	if (components.size() != 3) {
		throw InvalidInputException("Distributed Paimon INSERT has an invalid Vane task attempt identity");
	}
	ValidateVaneTaskIdentityComponent(components[0], "fragment execution identity");
	ValidateVaneTaskIdentityComponent(components[1], "task partition identity");
	ValidateVaneTaskIdentityComponent(components[2], "task attempt identity");
	return prefix + components[0] + "." + components[1];
}

static bool IsCanonicalUUID(const string &value) {
	hugeint_t parsed;
	return value.size() == BaseUUID::STRING_SIZE && UUID::FromString(value, parsed, true);
}

static string SchemaFingerprint(const string &schema_json) {
	MD5Context context;
	context.Add(schema_json);
	return context.FinishHex();
}

static string CompactUUID(const string &uuid) {
	string result;
	result.reserve(uuid.size());
	for (auto character : uuid) {
		if (character != '-') {
			result.push_back(character);
		}
	}
	return result;
}

static string JoinPath(const string &parent, const string &child) {
	if (parent.empty() || child.empty()) {
		throw InternalException("Distributed Paimon INSERT cannot join an empty path");
	}
	if (parent.back() == '/' || parent.back() == '\\') {
		return parent + child;
	}
	return parent + "/" + child;
}

static string AttemptDigest(const string &query_id, const string &task_attempt_id) {
	MD5Context context;
	context.Add(query_id);
	const data_t separator = 0;
	context.Add(&separator, 1);
	context.Add(task_attempt_id);
	return context.FinishHex();
}

static string WorkerCommitUser(const PaimonDistributedInsertTransport &transport,
                               const DistributedWriteTaskContext &task) {
	return "vane-" + CompactUUID(transport.operation_id) + "-" + AttemptDigest(task.query_id, task.task_attempt_id);
}

static string WorkerDataFilePrefix(const PaimonDistributedInsertTransport &transport,
                                   const DistributedWriteTaskContext &task) {
	return "vane_" + CompactUUID(transport.operation_id) + "_" + AttemptDigest(task.query_id, task.task_attempt_id);
}

static string OperationAttemptManifestDirectory(const PaimonDistributedInsertTransport &transport) {
	return JoinPath(transport.table_path, ".vane/paimon/" + CompactUUID(transport.operation_id));
}

static string AttemptManifestName(const DistributedWriteTaskContext &task) {
	return AttemptDigest(task.query_id, task.task_attempt_id) + PAIMON_DISTRIBUTED_ATTEMPT_MANIFEST_SUFFIX;
}

static string AttemptManifestPath(const PaimonDistributedInsertTransport &transport,
                                  const DistributedWriteTaskContext &task) {
	return JoinPath(OperationAttemptManifestDirectory(transport), AttemptManifestName(task));
}

static string OperationRoot(const PaimonDistributedInsertTransport &transport) {
	return JoinPath(transport.table_path, ".vane/paimon/" + string(PAIMON_DISTRIBUTED_OPERATION_DIRECTORY));
}

static string OperationMarkerPath(const PaimonDistributedInsertTransport &transport) {
	return JoinPath(OperationRoot(transport),
	                CompactUUID(transport.operation_id) + PAIMON_DISTRIBUTED_OPERATION_MARKER_SUFFIX);
}

static string RecoveryRoot(const PaimonDistributedInsertTransport &transport) {
	return JoinPath(transport.table_path, ".vane/paimon/" + string(PAIMON_DISTRIBUTED_RECOVERY_DIRECTORY));
}

static string RecoveryIntentName(const PaimonDistributedInsertTransport &transport) {
	return CompactUUID(transport.operation_id) + PAIMON_DISTRIBUTED_RECOVERY_INTENT_SUFFIX;
}

static string RecoveryIntentPath(const PaimonDistributedInsertTransport &transport) {
	return JoinPath(RecoveryRoot(transport), RecoveryIntentName(transport));
}

static string ExpectedFragmentId(const PaimonDistributedInsertTransport &transport, const string &task_attempt_id) {
	return transport.operation_id + ":" + task_attempt_id;
}

static int64_t CreateCommitIdentifier(const string &operation_id) {
	auto value = static_cast<uint64_t>(Hash(operation_id.c_str(), operation_id.size()));
	value &= static_cast<uint64_t>(NumericLimits<int64_t>::Maximum());
	if (value == 0) {
		value = 1;
	}
	return static_cast<int64_t>(value);
}

static bool IsPortableWriteOption(const string &key) {
	return key == paimon::Options::MANIFEST_FORMAT || key == paimon::Options::FILE_FORMAT;
}

static map<string, string> GetPortableWriteOptions(const map<string, string> &options) {
	map<string, string> result;
	for (const auto &key : {paimon::Options::MANIFEST_FORMAT, paimon::Options::FILE_FORMAT}) {
		auto entry = options.find(key);
		if (entry != options.end()) {
			result.emplace(entry->first, entry->second);
		}
	}
	return result;
}

static void ValidateTransport(const PaimonDistributedInsertTransport &transport) {
	if (!IsCanonicalUUID(transport.operation_id) || transport.database_name.empty() || transport.table_name.empty() ||
	    transport.table_uuid.empty() || transport.table_path.empty() || transport.table_schema_json.empty() ||
	    transport.schema_id < 0 || transport.commit_identifier <= 0 || transport.input_types.empty() ||
	    transport.input_types.size() != transport.input_names.size() || transport.null_part_name.empty()) {
		throw SerializationException("Distributed Paimon INSERT bind has incomplete target state");
	}
	if ((!transport.has_snapshot && transport.snapshot_id != 0) ||
	    (transport.has_snapshot && transport.snapshot_id <= 0)) {
		throw SerializationException("Distributed Paimon INSERT bind has non-canonical snapshot state");
	}

	unordered_set<string> field_names;
	for (idx_t index = 0; index < transport.input_types.size(); index++) {
		if (transport.input_types[index].id() == LogicalTypeId::INVALID || transport.input_names[index].empty() ||
		    !field_names.insert(transport.input_names[index]).second) {
			throw SerializationException("Distributed Paimon INSERT bind has an invalid input schema");
		}
	}
	unordered_set<string> part_keys;
	for (const auto &part_key : transport.part_keys) {
		if (part_key.empty() || field_names.find(part_key) == field_names.end() || !part_keys.insert(part_key).second) {
			throw SerializationException("Distributed Paimon INSERT bind has an invalid partition key");
		}
	}
	for (const auto &option : transport.portable_options) {
		if (!IsPortableWriteOption(option.first) || option.second.empty()) {
			throw SerializationException("Distributed Paimon INSERT bind contains non-portable option '%s'",
			                             option.first);
		}
	}
}

static void SerializeTransport(Serializer &serializer, const PaimonDistributedInsertTransport &transport) {
	ValidateTransport(transport);
	serializer.WriteProperty(1, "protocol_version", PAIMON_DISTRIBUTED_INSERT_PROTOCOL_VERSION);
	serializer.WriteProperty(2, "operation_id", transport.operation_id);
	serializer.WriteProperty(3, "database_name", transport.database_name);
	serializer.WriteProperty(4, "table_name", transport.table_name);
	serializer.WriteProperty(5, "table_path", transport.table_path);
	serializer.WriteProperty(6, "table_schema_json", transport.table_schema_json);
	serializer.WriteProperty(7, "schema_id", transport.schema_id);
	serializer.WriteProperty(8, "has_snapshot", transport.has_snapshot);
	serializer.WriteProperty(9, "snapshot_id", transport.has_snapshot ? transport.snapshot_id : 0);
	serializer.WriteProperty(10, "commit_identifier", transport.commit_identifier);
	serializer.WriteProperty(11, "input_types", transport.input_types);
	serializer.WriteProperty(12, "input_names", transport.input_names);
	serializer.WriteProperty(13, "part_keys", transport.part_keys);
	serializer.WriteProperty(14, "null_part_name", transport.null_part_name);
	serializer.WriteProperty(15, "portable_options", transport.portable_options);
	serializer.WriteProperty(16, "append_only", transport.append_only);
	serializer.WriteProperty(17, "table_uuid", transport.table_uuid);
}

static string SerializeTransport(const PaimonDistributedInsertTransport &transport) {
	MemoryStream stream(Allocator::DefaultAllocator());
	BinarySerializer serializer(stream);
	serializer.Begin();
	SerializeTransport(serializer, transport);
	serializer.End();
	return string(reinterpret_cast<const char *>(stream.GetData()), stream.GetPosition());
}

static PaimonDistributedInsertTransport DeserializeTransport(const string &bytes) {
	if (bytes.empty()) {
		throw SerializationException("Cannot deserialize an empty distributed Paimon INSERT bind");
	}
	vector<data_t> buffer(bytes.begin(), bytes.end());
	MemoryStream stream(buffer.data(), buffer.size());
	BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	auto protocol_version = deserializer.ReadProperty<uint32_t>(1, "protocol_version");
	if (protocol_version != PAIMON_DISTRIBUTED_INSERT_PROTOCOL_VERSION) {
		throw SerializationException("Distributed Paimon INSERT bind has unsupported protocol version %u",
		                             protocol_version);
	}
	PaimonDistributedInsertTransport result;
	result.operation_id = deserializer.ReadProperty<string>(2, "operation_id");
	result.database_name = deserializer.ReadProperty<string>(3, "database_name");
	result.table_name = deserializer.ReadProperty<string>(4, "table_name");
	result.table_path = deserializer.ReadProperty<string>(5, "table_path");
	result.table_schema_json = deserializer.ReadProperty<string>(6, "table_schema_json");
	result.schema_id = deserializer.ReadProperty<int64_t>(7, "schema_id");
	result.has_snapshot = deserializer.ReadProperty<bool>(8, "has_snapshot");
	result.snapshot_id = deserializer.ReadProperty<int64_t>(9, "snapshot_id");
	result.commit_identifier = deserializer.ReadProperty<int64_t>(10, "commit_identifier");
	result.input_types = deserializer.ReadProperty<vector<LogicalType>>(11, "input_types");
	result.input_names = deserializer.ReadProperty<vector<string>>(12, "input_names");
	result.part_keys = deserializer.ReadProperty<vector<string>>(13, "part_keys");
	result.null_part_name = deserializer.ReadProperty<string>(14, "null_part_name");
	result.portable_options = deserializer.ReadProperty<map<string, string>>(15, "portable_options");
	result.append_only = deserializer.ReadProperty<bool>(16, "append_only");
	result.table_uuid = deserializer.ReadProperty<string>(17, "table_uuid");
	deserializer.End();
	ValidateTransport(result);
	return result;
}

static void ValidateRecoveryIntent(const PaimonDistributedRecoveryIntent &intent) {
	ValidateTransport(intent.transport);
	if (intent.action != PaimonDistributedRecoveryAction::COMMIT &&
	    intent.action != PaimonDistributedRecoveryAction::ABORT) {
		throw SerializationException("Distributed Paimon recovery intent has an invalid action");
	}
	if ((intent.action == PaimonDistributedRecoveryAction::COMMIT &&
	     (intent.selected_attempt_manifests.empty() || intent.row_count == 0)) ||
	    (intent.action == PaimonDistributedRecoveryAction::ABORT && intent.row_count != 0)) {
		throw SerializationException("Distributed Paimon recovery intent has non-canonical selected state");
	}
	for (const auto &manifest : intent.selected_attempt_manifests) {
		if (!IsAttemptManifestName(manifest.first) || manifest.second.empty()) {
			throw SerializationException("Distributed Paimon recovery intent has an invalid selected manifest");
		}
	}
}

static string SerializeRecoveryIntent(const PaimonDistributedRecoveryIntent &intent) {
	ValidateRecoveryIntent(intent);
	MemoryStream stream(Allocator::DefaultAllocator());
	BinarySerializer serializer(stream);
	serializer.Begin();
	serializer.WriteProperty(1, "protocol_version", PAIMON_DISTRIBUTED_INSERT_PROTOCOL_VERSION);
	serializer.WriteProperty(2, "transport", SerializeTransport(intent.transport));
	serializer.WriteProperty(3, "action", static_cast<uint8_t>(intent.action));
	serializer.WriteProperty(4, "selected_attempt_manifests", intent.selected_attempt_manifests);
	serializer.WriteProperty(5, "row_count", intent.row_count);
	serializer.End();
	return string(reinterpret_cast<const char *>(stream.GetData()), stream.GetPosition());
}

static PaimonDistributedRecoveryIntent DeserializeRecoveryIntent(const string &bytes) {
	if (bytes.empty()) {
		throw SerializationException("Cannot deserialize an empty distributed Paimon recovery intent");
	}
	vector<data_t> buffer(bytes.begin(), bytes.end());
	MemoryStream stream(buffer.data(), buffer.size());
	BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	auto protocol_version = deserializer.ReadProperty<uint32_t>(1, "protocol_version");
	if (protocol_version != PAIMON_DISTRIBUTED_INSERT_PROTOCOL_VERSION) {
		throw SerializationException("Distributed Paimon recovery intent has unsupported protocol version %u",
		                             protocol_version);
	}
	PaimonDistributedRecoveryIntent result;
	result.transport = DeserializeTransport(deserializer.ReadProperty<string>(2, "transport"));
	result.action = static_cast<PaimonDistributedRecoveryAction>(deserializer.ReadProperty<uint8_t>(3, "action"));
	result.selected_attempt_manifests = deserializer.ReadProperty<map<string, string>>(4, "selected_attempt_manifests");
	result.row_count = deserializer.ReadProperty<idx_t>(5, "row_count");
	deserializer.End();
	ValidateRecoveryIntent(result);
	return result;
}

static void SerializeCommitEnvelope(Serializer &serializer, const PaimonDistributedCommitEnvelope &envelope) {
	serializer.WriteProperty(1, "protocol_version", PAIMON_DISTRIBUTED_INSERT_PROTOCOL_VERSION);
	serializer.WriteProperty(2, "operation_id", envelope.operation_id);
	serializer.WriteProperty(3, "table_path", envelope.table_path);
	serializer.WriteProperty(4, "schema_fingerprint", envelope.schema_fingerprint);
	serializer.WriteProperty(5, "schema_id", envelope.schema_id);
	serializer.WriteProperty(6, "has_snapshot", envelope.has_snapshot);
	serializer.WriteProperty(7, "snapshot_id", envelope.has_snapshot ? envelope.snapshot_id : 0);
	serializer.WriteProperty(8, "commit_identifier", envelope.commit_identifier);
	serializer.WriteProperty(9, "query_id", envelope.query_id);
	serializer.WriteProperty(10, "task_attempt_id", envelope.task_attempt_id);
	serializer.WriteProperty(11, "writer_commit_user", envelope.writer_commit_user);
	serializer.WriteProperty(12, "commit_message_version", envelope.commit_message_version);
	serializer.WriteProperty(13, "message_count", envelope.message_count);
	serializer.WriteProperty(14, "row_count", envelope.row_count);
	serializer.WriteProperty(15, "serialized_messages", envelope.serialized_messages);
	serializer.WriteProperty(16, "table_uuid", envelope.table_uuid);
}

static string SerializeCommitEnvelope(const PaimonDistributedCommitEnvelope &envelope) {
	MemoryStream stream(Allocator::DefaultAllocator());
	BinarySerializer serializer(stream);
	serializer.Begin();
	SerializeCommitEnvelope(serializer, envelope);
	serializer.End();
	return string(reinterpret_cast<const char *>(stream.GetData()), stream.GetPosition());
}

static PaimonDistributedCommitEnvelope DeserializeCommitEnvelope(const string &bytes) {
	if (bytes.empty()) {
		throw SerializationException("Cannot deserialize an empty distributed Paimon commit fragment");
	}
	vector<data_t> buffer(bytes.begin(), bytes.end());
	MemoryStream stream(buffer.data(), buffer.size());
	BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	auto protocol_version = deserializer.ReadProperty<uint32_t>(1, "protocol_version");
	if (protocol_version != PAIMON_DISTRIBUTED_INSERT_PROTOCOL_VERSION) {
		throw SerializationException("Distributed Paimon commit fragment has unsupported protocol version %u",
		                             protocol_version);
	}
	PaimonDistributedCommitEnvelope result;
	result.operation_id = deserializer.ReadProperty<string>(2, "operation_id");
	result.table_path = deserializer.ReadProperty<string>(3, "table_path");
	result.schema_fingerprint = deserializer.ReadProperty<string>(4, "schema_fingerprint");
	result.schema_id = deserializer.ReadProperty<int64_t>(5, "schema_id");
	result.has_snapshot = deserializer.ReadProperty<bool>(6, "has_snapshot");
	result.snapshot_id = deserializer.ReadProperty<int64_t>(7, "snapshot_id");
	result.commit_identifier = deserializer.ReadProperty<int64_t>(8, "commit_identifier");
	result.query_id = deserializer.ReadProperty<string>(9, "query_id");
	result.task_attempt_id = deserializer.ReadProperty<string>(10, "task_attempt_id");
	result.writer_commit_user = deserializer.ReadProperty<string>(11, "writer_commit_user");
	result.commit_message_version = deserializer.ReadProperty<int32_t>(12, "commit_message_version");
	result.message_count = deserializer.ReadProperty<idx_t>(13, "message_count");
	result.row_count = deserializer.ReadProperty<idx_t>(14, "row_count");
	result.serialized_messages = deserializer.ReadProperty<string>(15, "serialized_messages");
	result.table_uuid = deserializer.ReadProperty<string>(16, "table_uuid");
	deserializer.End();
	if (!IsCanonicalUUID(result.operation_id) || result.table_uuid.empty() || result.table_path.empty() ||
	    result.schema_fingerprint.size() != 32 || result.schema_id < 0 || result.commit_identifier <= 0 ||
	    result.query_id.empty() || result.task_attempt_id.empty() || result.writer_commit_user.empty() ||
	    result.commit_message_version <= 0 || result.message_count == 0 || result.row_count == 0 ||
	    result.serialized_messages.empty() || (!result.has_snapshot && result.snapshot_id != 0) ||
	    (result.has_snapshot && result.snapshot_id <= 0)) {
		throw SerializationException("Distributed Paimon commit fragment contains invalid identity or payload state");
	}
	return result;
}

static string LoadTableUUID(paimon::Catalog &catalog, const paimon::Identifier &table_identifier) {
	auto table_result = catalog.GetTable(table_identifier);
	if (!table_result.ok()) {
		throw IOException(table_result.status().ToString());
	}
	auto table = std::move(table_result).value();
	if (!table) {
		throw IOException("Paimon table %s has no stable table identity", table_identifier.ToString());
	}
	auto table_uuid = table->Uuid();
	if (table_uuid.empty()) {
		throw IOException("Paimon table %s has an empty table UUID", table_identifier.ToString());
	}
	return table_uuid;
}

static PaimonDistributedTargetState LoadTargetState(PaimonCatalog &catalog,
                                                    const paimon::Identifier &table_identifier) {
	auto &paimon_catalog = catalog.GetPaimonCatalog();
	auto exists_result = paimon_catalog.TableExists(table_identifier);
	if (!exists_result.ok()) {
		throw IOException(exists_result.status().ToString());
	}
	if (!exists_result.value()) {
		throw TransactionException("Paimon table %s no longer exists", table_identifier.ToString());
	}

	PaimonDistributedTargetState result;
	result.table_uuid = LoadTableUUID(paimon_catalog, table_identifier);

	auto path_result = paimon_catalog.GetTableLocation(table_identifier);
	if (!path_result.ok()) {
		throw IOException(path_result.status().ToString());
	}
	result.table_path = std::move(path_result).value();

	auto schema_result = paimon_catalog.LoadTableSchema(table_identifier);
	if (!schema_result.ok()) {
		throw IOException(schema_result.status().ToString());
	}
	auto table_schema = std::move(schema_result).value();
	auto data_schema = std::dynamic_pointer_cast<paimon::DataSchema>(table_schema);
	if (!data_schema) {
		throw IOException("Failed to resolve Paimon data schema for distributed INSERT");
	}
	result.schema_id = data_schema->Id();
	result.append_only = data_schema->PrimaryKeys().empty();
	result.field_names = table_schema->FieldNames();
	auto &schema_part_keys = data_schema->PartitionKeys();
	result.part_keys.assign(schema_part_keys.begin(), schema_part_keys.end());
	result.null_part_name = "__DEFAULT_PARTITION__";
	auto default_partition = data_schema->Options().find(paimon::Options::PARTITION_DEFAULT_NAME);
	if (default_partition != data_schema->Options().end()) {
		result.null_part_name = default_partition->second;
	}
	auto schema_json_result = table_schema->GetJsonSchema();
	if (!schema_json_result.ok()) {
		throw IOException(schema_json_result.status().ToString());
	}
	result.table_schema_json = std::move(schema_json_result).value();

	auto snapshots_result = paimon_catalog.ListSnapshots(table_identifier);
	if (!snapshots_result.ok()) {
		throw IOException(snapshots_result.status().ToString());
	}
	auto snapshots = std::move(snapshots_result).value();
	if (!snapshots.empty()) {
		result.has_snapshot = true;
		result.snapshot_id = snapshots.back().snapshot_id;
	}
	if (LoadTableUUID(paimon_catalog, table_identifier) != result.table_uuid) {
		throw TransactionException("Paimon table %s was replaced while resolving the distributed INSERT target",
		                           table_identifier.ToString());
	}
	return result;
}

static map<string, string> BuildRuntimeOptions(ClientContext &context,
                                               const PaimonDistributedInsertTransport &transport,
                                               optional_ptr<const DistributedWriteTaskContext> task = nullptr) {
	unordered_map<string, Value> local_options;
	auto result = PaimonCatalog::GetPaimonOptions(context, transport.table_path, local_options);
	for (const auto &option : transport.portable_options) {
		result[option.first] = option.second;
	}
	if (task) {
		result[paimon::Options::DATA_FILE_PREFIX] = WorkerDataFilePrefix(transport, *task);
	}
	return result;
}

static unique_ptr<paimon::FileStoreCommit> CreateCommitter(const PaimonDistributedInsertTransport &transport,
                                                           const map<string, string> &options,
                                                           const string &commit_user, bool conflict_check) {
	paimon::CommitContextBuilder builder(transport.table_path, commit_user);
	builder.SetOptions(options).AppendCommitCheckConflict(conflict_check);
	auto context_result = builder.Finish();
	if (!context_result.ok()) {
		throw IOException(context_result.status().ToString());
	}
	auto committer_result = paimon::FileStoreCommit::Create(std::move(context_result).value());
	if (!committer_result.ok()) {
		throw IOException(committer_result.status().ToString());
	}
	return unique_ptr<paimon::FileStoreCommit>(std::move(committer_result).value().release());
}

static void AbortMessages(const PaimonDistributedInsertTransport &transport, const map<string, string> &options,
                          const string &commit_user,
                          const std::vector<std::shared_ptr<paimon::CommitMessage>> &messages) {
	if (messages.empty()) {
		return;
	}
	auto committer = CreateCommitter(transport, options, commit_user, false);
	auto status = committer->Abort(messages);
	if (!status.ok()) {
		throw IOException(status.ToString());
	}
}

static void BestEffortAbortMessages(const PaimonDistributedInsertTransport &transport,
                                    const map<string, string> &options, const string &commit_user,
                                    const std::vector<std::shared_ptr<paimon::CommitMessage>> &messages) noexcept {
	try {
		AbortMessages(transport, options, commit_user, messages);
	} catch (...) {
	}
}

static string FileName(const string &path) {
	auto separator = path.find_last_of("/\\");
	return separator == string::npos ? path : path.substr(separator + 1);
}

static std::unique_ptr<paimon::FileSystem> GetFileSystem(const PaimonDistributedInsertTransport &transport,
                                                         const map<string, string> &options) {
	auto file_system_entry = options.find(paimon::Options::FILE_SYSTEM);
	if (file_system_entry == options.end() || file_system_entry->second.empty()) {
		throw IOException("Distributed Paimon INSERT cleanup has no file-system identity");
	}
	auto file_system_result = paimon::FileSystemFactory::Get(file_system_entry->second, transport.table_path, options);
	if (!file_system_result.ok()) {
		throw IOException(file_system_result.status().ToString());
	}
	return std::move(file_system_result).value();
}

static string SerializeOperationMarker(const PaimonDistributedInsertTransport &transport) {
	MemoryStream stream(Allocator::DefaultAllocator());
	BinarySerializer serializer(stream);
	serializer.Begin();
	serializer.WriteProperty(1, "protocol_version", PAIMON_DISTRIBUTED_INSERT_PROTOCOL_VERSION);
	serializer.WriteProperty(2, "operation_id", transport.operation_id);
	serializer.WriteProperty(3, "table_uuid", transport.table_uuid);
	serializer.WriteProperty(4, "table_path", transport.table_path);
	serializer.End();
	return string(reinterpret_cast<const char *>(stream.GetData()), stream.GetPosition());
}

static bool RecoveryIntentMatches(paimon::FileSystem &file_system, const PaimonDistributedRecoveryIntent &intent,
                                  const string &payload) {
	const auto path = RecoveryIntentPath(intent.transport);
	auto exists_result = file_system.Exists(path);
	if (!exists_result.ok()) {
		throw IOException("Failed to inspect distributed Paimon recovery intent: %s",
		                  exists_result.status().ToString());
	}
	if (!exists_result.value()) {
		return false;
	}
	string stored_payload;
	auto status = file_system.ReadFile(path, &stored_payload);
	if (!status.ok()) {
		throw IOException("Failed to read distributed Paimon recovery intent: %s", status.ToString());
	}
	if (stored_payload != payload) {
		throw InvalidInputException("Distributed Paimon recovery intent does not match its operation");
	}
	return true;
}

static void PublishRecoveryIntent(const PaimonDistributedRecoveryIntent &intent, const string &payload,
                                  const map<string, string> &options, bool &publication_may_exist) {
	publication_may_exist = false;
	auto file_system = GetFileSystem(intent.transport, options);
	auto status = file_system->AtomicStore(RecoveryIntentPath(intent.transport), payload);
	publication_may_exist = true;
	if (status.ok() || RecoveryIntentMatches(*file_system, intent, payload)) {
		publication_may_exist = true;
		return;
	}
	publication_may_exist = false;
	throw IOException("Failed to publish distributed Paimon recovery intent: %s", status.ToString());
}

static void DeleteRecoveryIntent(paimon::FileSystem &file_system, const PaimonDistributedInsertTransport &transport) {
	const auto path = RecoveryIntentPath(transport);
	auto status = file_system.Delete(path, false);
	auto exists_result = file_system.Exists(path);
	if (!exists_result.ok()) {
		throw IOException("Failed to verify distributed Paimon recovery-intent cleanup: %s",
		                  exists_result.status().ToString());
	}
	if (exists_result.value()) {
		if (!status.ok() && !status.IsNotExist()) {
			throw IOException("Failed to delete distributed Paimon recovery intent: %s", status.ToString());
		}
		throw IOException("Distributed Paimon recovery intent remained after deletion");
	}
	(void)file_system.Delete(RecoveryRoot(transport), false);
}

static bool BestEffortDeleteRecoveryIntent(const PaimonDistributedInsertTransport &transport,
                                           const map<string, string> &options) noexcept {
	try {
		auto file_system = GetFileSystem(transport, options);
		DeleteRecoveryIntent(*file_system, transport);
		return true;
	} catch (...) {
		return false;
	}
}

static bool OperationIsOpen(paimon::FileSystem &file_system, const PaimonDistributedInsertTransport &transport) {
	const auto path = OperationMarkerPath(transport);
	auto exists_result = file_system.Exists(path);
	if (!exists_result.ok()) {
		throw IOException("Failed to inspect distributed Paimon operation marker: %s",
		                  exists_result.status().ToString());
	}
	if (!exists_result.value()) {
		return false;
	}
	string payload;
	auto status = file_system.ReadFile(path, &payload);
	if (!status.ok()) {
		throw IOException("Failed to read distributed Paimon operation marker: %s", status.ToString());
	}
	if (payload != SerializeOperationMarker(transport)) {
		throw InvalidInputException("Distributed Paimon INSERT operation marker does not match its target");
	}
	return true;
}

static void OpenOperation(const PaimonDistributedInsertTransport &transport, const map<string, string> &options) {
	auto file_system = GetFileSystem(transport, options);
	const auto path = OperationMarkerPath(transport);
	auto status = file_system->AtomicStore(path, SerializeOperationMarker(transport));
	if (status.ok() || OperationIsOpen(*file_system, transport)) {
		return;
	}
	throw IOException("Failed to publish distributed Paimon operation marker: %s", status.ToString());
}

static void BestEffortDeleteOperationDirectories(paimon::FileSystem &file_system,
                                                 const PaimonDistributedInsertTransport &transport) noexcept {
	try {
		(void)file_system.Delete(OperationRoot(transport), false);
	} catch (...) {
	}
}

static void CloseOperation(const PaimonDistributedInsertTransport &transport, const map<string, string> &options) {
	auto file_system = GetFileSystem(transport, options);
	const auto path = OperationMarkerPath(transport);
	auto status = file_system->Delete(path, false);
	if (!OperationIsOpen(*file_system, transport)) {
		BestEffortDeleteOperationDirectories(*file_system, transport);
		return;
	}
	if (!status.ok() && !status.IsNotExist()) {
		throw IOException("Failed to close distributed Paimon operation: %s", status.ToString());
	}
	throw IOException("Distributed Paimon operation remained open after its marker was deleted");
}

static bool BestEffortCloseOperation(const PaimonDistributedInsertTransport &transport,
                                     const map<string, string> &options) noexcept {
	try {
		CloseOperation(transport, options);
		return true;
	} catch (...) {
		return false;
	}
}

static void BestEffortDeleteAttemptManifest(paimon::FileSystem &file_system,
                                            const PaimonDistributedInsertTransport &transport,
                                            const DistributedWriteTaskContext &task) noexcept {
	try {
		(void)file_system.Delete(AttemptManifestPath(transport, task), false);
		(void)file_system.Delete(OperationAttemptManifestDirectory(transport), false);
	} catch (...) {
	}
}

static bool IsAttemptManifestName(const string &name) {
	const string suffix = PAIMON_DISTRIBUTED_ATTEMPT_MANIFEST_SUFFIX;
	if (name.size() != PAIMON_DISTRIBUTED_ATTEMPT_DIGEST_SIZE + suffix.size() ||
	    name.compare(PAIMON_DISTRIBUTED_ATTEMPT_DIGEST_SIZE, suffix.size(), suffix) != 0) {
		return false;
	}
	for (idx_t index = 0; index < PAIMON_DISTRIBUTED_ATTEMPT_DIGEST_SIZE; index++) {
		const auto character = name[index];
		if (!StringUtil::CharacterIsDigit(character) && (character < 'a' || character > 'f')) {
			return false;
		}
	}
	return true;
}

static bool IsRecoveryIntentName(const string &name) {
	const string suffix = PAIMON_DISTRIBUTED_RECOVERY_INTENT_SUFFIX;
	if (name.size() != PAIMON_DISTRIBUTED_ATTEMPT_DIGEST_SIZE + suffix.size() ||
	    name.compare(PAIMON_DISTRIBUTED_ATTEMPT_DIGEST_SIZE, suffix.size(), suffix) != 0) {
		return false;
	}
	for (idx_t index = 0; index < PAIMON_DISTRIBUTED_ATTEMPT_DIGEST_SIZE; index++) {
		const auto character = name[index];
		if (!StringUtil::CharacterIsDigit(character) && (character < 'a' || character > 'f')) {
			return false;
		}
	}
	return true;
}

static void ValidateAttemptManifestEnvelope(const PaimonDistributedInsertTransport &transport,
                                            const PaimonDistributedCommitEnvelope &envelope,
                                            const string &manifest_name) {
	DistributedWriteTaskContext task {envelope.query_id, envelope.task_attempt_id};
	VaneLogicalTaskIdentity(task.query_id, task.task_attempt_id);
	if (manifest_name != AttemptManifestName(task) || envelope.operation_id != transport.operation_id ||
	    envelope.table_uuid != transport.table_uuid || envelope.table_path != transport.table_path ||
	    envelope.schema_fingerprint != SchemaFingerprint(transport.table_schema_json) ||
	    envelope.schema_id != transport.schema_id || envelope.has_snapshot != transport.has_snapshot ||
	    envelope.snapshot_id != transport.snapshot_id || envelope.commit_identifier != transport.commit_identifier ||
	    envelope.writer_commit_user != WorkerCommitUser(transport, task) ||
	    envelope.commit_message_version != paimon::CommitMessage::CurrentVersion()) {
		throw InvalidInputException("Distributed Paimon INSERT attempt manifest does not match its operation");
	}
}

static std::vector<std::shared_ptr<paimon::CommitMessage>>
DeserializeManifestMessages(const PaimonDistributedCommitEnvelope &envelope) {
	if (envelope.serialized_messages.size() > static_cast<idx_t>(NumericLimits<int32_t>::Maximum())) {
		throw InvalidInputException("Distributed Paimon INSERT attempt manifest exceeds the Paimon codec limit");
	}
	auto messages_result = paimon::CommitMessage::DeserializeList(
	    envelope.commit_message_version, envelope.serialized_messages.data(),
	    NumericCast<int32_t>(envelope.serialized_messages.size()), paimon::GetDefaultPool());
	if (!messages_result.ok()) {
		throw InvalidInputException("Failed to deserialize distributed Paimon attempt manifest: %s",
		                            messages_result.status().ToString());
	}
	auto messages = std::move(messages_result).value();
	if (messages.size() != envelope.message_count || messages.empty()) {
		throw InvalidInputException("Distributed Paimon INSERT attempt manifest has an invalid message count");
	}
	for (const auto &message : messages) {
		if (!message) {
			throw InvalidInputException("Distributed Paimon INSERT attempt manifest contains an empty message");
		}
	}
	return messages;
}

static map<string, string> CopySelectedAttemptManifests(const PaimonSelectedAttemptManifests &selected_manifests) {
	map<string, string> result;
	for (const auto &manifest : selected_manifests) {
		if (!IsAttemptManifestName(manifest.first) || !manifest.second || manifest.second->empty()) {
			throw InternalException("Distributed Paimon INSERT has an invalid selected attempt manifest");
		}
		result.emplace(manifest.first, *manifest.second);
	}
	return result;
}

static PaimonDecodedDistributedCommit DecodeRecoveryIntent(const PaimonDistributedRecoveryIntent &intent) {
	PaimonDecodedDistributedCommit result;
	unordered_set<string> logical_task_ids;
	unordered_set<string> canonical_messages;
	for (const auto &manifest : intent.selected_attempt_manifests) {
		auto envelope = DeserializeCommitEnvelope(manifest.second);
		ValidateAttemptManifestEnvelope(intent.transport, envelope, manifest.first);
		auto logical_task_id = VaneLogicalTaskIdentity(envelope.query_id, envelope.task_attempt_id);
		if (!logical_task_ids.insert(std::move(logical_task_id)).second &&
		    intent.action == PaimonDistributedRecoveryAction::COMMIT) {
			throw InvalidInputException(
			    "Distributed Paimon recovery intent selected multiple attempts for one logical task");
		}
		auto messages = DeserializeManifestMessages(envelope);
		if (messages.size() > NumericLimits<idx_t>::Maximum() - result.messages.size()) {
			throw InvalidInputException("Distributed Paimon recovery intent has too many commit messages");
		}
		for (auto &message : messages) {
			auto canonical_result = paimon::CommitMessage::Serialize(message, paimon::GetDefaultPool());
			if (!canonical_result.ok()) {
				throw InvalidInputException("Failed to canonicalize distributed Paimon recovery message: %s",
				                            canonical_result.status().ToString());
			}
			if (!canonical_messages.insert(std::move(canonical_result).value()).second) {
				if (intent.action == PaimonDistributedRecoveryAction::COMMIT) {
					throw InvalidInputException(
					    "Distributed Paimon recovery intent contains a duplicate commit message");
				}
				continue;
			}
			result.messages.push_back(std::move(message));
		}
		result.row_count = CheckedAdd(result.row_count, envelope.row_count, "recovery row count");
		result.selected_attempt_manifests.emplace(manifest.first, optional_ptr<const string>(&manifest.second));
	}
	if (intent.action == PaimonDistributedRecoveryAction::COMMIT &&
	    (result.row_count != intent.row_count || result.messages.empty())) {
		throw InvalidInputException("Distributed Paimon recovery intent does not match its selected messages");
	}
	return result;
}

static void AbortAndRetractAttemptManifest(paimon::FileSystem &file_system,
                                           const PaimonDistributedInsertTransport &transport,
                                           const map<string, string> &options, const DistributedWriteTaskContext &task,
                                           const string &commit_user,
                                           std::vector<std::shared_ptr<paimon::CommitMessage>> &messages) {
	AbortMessages(transport, options, commit_user, messages);
	messages.clear();
	BestEffortDeleteAttemptManifest(file_system, transport, task);
}

static void WriteAttemptManifest(const PaimonDistributedInsertTransport &transport, const map<string, string> &options,
                                 const DistributedWriteTaskContext &task, const string &commit_user,
                                 std::vector<std::shared_ptr<paimon::CommitMessage>> &messages, const string &payload) {
	auto file_system = GetFileSystem(transport, options);
	if (!OperationIsOpen(*file_system, transport)) {
		AbortMessages(transport, options, commit_user, messages);
		messages.clear();
		throw TransactionException("Distributed Paimon INSERT operation is closed");
	}
	const auto path = AttemptManifestPath(transport, task);
	auto status = file_system->AtomicStore(path, payload);
	if (!status.ok()) {
		throw IOException("Failed to publish distributed Paimon attempt manifest: %s", status.ToString());
	}
	bool operation_open = false;
	try {
		operation_open = OperationIsOpen(*file_system, transport);
	} catch (...) {
		auto marker_error = std::current_exception();
		try {
			AbortAndRetractAttemptManifest(*file_system, transport, options, task, commit_user, messages);
		} catch (...) {
		}
		std::rethrow_exception(marker_error);
	}
	if (!operation_open) {
		AbortAndRetractAttemptManifest(*file_system, transport, options, task, commit_user, messages);
		throw TransactionException("Distributed Paimon INSERT operation closed while publishing an attempt");
	}
}

static void CleanupAttemptManifests(const PaimonDistributedInsertTransport &transport,
                                    const map<string, string> &options,
                                    optional_ptr<const PaimonSelectedAttemptManifests> retained_manifests = nullptr,
                                    bool require_all_retained = true) {
	if (retained_manifests) {
		for (const auto &manifest : *retained_manifests) {
			if (!IsAttemptManifestName(manifest.first) || !manifest.second || manifest.second->empty()) {
				throw InternalException("Distributed Paimon INSERT has an invalid selected attempt manifest");
			}
		}
	}

	auto file_system = GetFileSystem(transport, options);
	const auto directory = OperationAttemptManifestDirectory(transport);
	std::vector<std::unique_ptr<paimon::BasicFileStatus>> entries;
	auto status = file_system->ListDir(directory, &entries);
	if (!status.ok()) {
		if (status.IsNotExist() && (!retained_manifests || retained_manifests->empty() || !require_all_retained)) {
			return;
		}
		throw IOException("Failed to list distributed Paimon attempt manifests: %s", status.ToString());
	}

	unordered_set<string> retained_found;
	for (auto &entry : entries) {
		if (!entry || entry->IsDir() || entry->GetPath().empty()) {
			throw IOException("Distributed Paimon attempt-manifest directory contains an invalid entry");
		}
		const auto path = entry->GetPath();
		const auto name = FileName(path);
		if (!IsAttemptManifestName(name)) {
			throw IOException("Distributed Paimon attempt-manifest directory contains an unknown artifact");
		}
		optional_ptr<const string> retained_payload;
		if (retained_manifests) {
			auto retained = retained_manifests->find(name);
			if (retained != retained_manifests->end()) {
				retained_payload = retained->second;
			}
		}
		string payload;
		status = file_system->ReadFile(path, &payload);
		if (!status.ok()) {
			// Once the operation marker is removed, an unselected worker may abort its messages and retract its own
			// manifest between this ListDir result and ReadFile. A selected manifest must remain byte-verifiable.
			if (status.IsNotExist() && !retained_payload) {
				continue;
			}
			throw IOException("Failed to read distributed Paimon attempt manifest: %s", status.ToString());
		}
		auto envelope = DeserializeCommitEnvelope(payload);
		ValidateAttemptManifestEnvelope(transport, envelope, name);
		if (retained_payload) {
			if (*retained_payload != payload) {
				throw InvalidInputException(
				    "Distributed Paimon INSERT selected result does not match its published attempt manifest");
			}
			retained_found.insert(name);
			continue;
		}
		auto messages = DeserializeManifestMessages(envelope);
		AbortMessages(transport, options, envelope.writer_commit_user, messages);
		status = file_system->Delete(path, false);
		if (!status.ok() && !status.IsNotExist()) {
			throw IOException("Failed to delete distributed Paimon attempt manifest: %s", status.ToString());
		}
	}

	if (retained_manifests && require_all_retained && retained_found.size() != retained_manifests->size()) {
		throw IOException("Distributed Paimon INSERT is missing a selected attempt manifest");
	}
	if (!retained_manifests || retained_manifests->empty()) {
		auto exists_result = file_system->Exists(directory);
		if (!exists_result.ok()) {
			throw IOException("Failed to inspect distributed Paimon attempt-manifest directory: %s",
			                  exists_result.status().ToString());
		}
		if (!exists_result.value()) {
			return;
		}
		status = file_system->Delete(directory, false);
		if (!status.ok() && !status.IsNotExist()) {
			throw IOException("Failed to delete distributed Paimon attempt-manifest directory: %s", status.ToString());
		}
	}
}

static void BestEffortCleanupAttemptManifests(const PaimonDistributedInsertTransport &transport,
                                              const map<string, string> &options) noexcept {
	try {
		CleanupAttemptManifests(transport, options);
	} catch (...) {
	}
}

static void DeleteSelectedAttemptManifests(const PaimonDistributedInsertTransport &transport,
                                           const map<string, string> &options,
                                           const PaimonSelectedAttemptManifests &selected_manifests) {
	for (const auto &manifest : selected_manifests) {
		if (!IsAttemptManifestName(manifest.first)) {
			throw InternalException("Distributed Paimon INSERT has an invalid selected attempt manifest");
		}
	}
	auto file_system = GetFileSystem(transport, options);
	const auto directory = OperationAttemptManifestDirectory(transport);
	for (const auto &manifest : selected_manifests) {
		auto status = file_system->Delete(JoinPath(directory, manifest.first), false);
		if (!status.ok() && !status.IsNotExist()) {
			throw IOException("Failed to delete distributed Paimon selected attempt manifest: %s", status.ToString());
		}
	}
	auto status = file_system->Delete(directory, false);
	if (!status.ok() && !status.IsNotExist()) {
		throw IOException("Failed to delete distributed Paimon attempt-manifest directory: %s", status.ToString());
	}
	auto exists_result = file_system->Exists(directory);
	if (!exists_result.ok()) {
		throw IOException("Failed to verify distributed Paimon attempt-manifest cleanup: %s",
		                  exists_result.status().ToString());
	}
	if (exists_result.value()) {
		throw IOException("Distributed Paimon attempt-manifest directory remained after cleanup");
	}
}

static bool
BestEffortDeleteSelectedAttemptManifests(const PaimonDistributedInsertTransport &transport,
                                         const map<string, string> &options,
                                         const PaimonSelectedAttemptManifests &selected_manifests) noexcept {
	try {
		DeleteSelectedAttemptManifests(transport, options, selected_manifests);
		return true;
	} catch (...) {
		return false;
	}
}

static bool TargetMatchesRecoveryBaseline(const PaimonDistributedTargetState &target,
                                          const PaimonDistributedInsertTransport &transport) {
	return target.table_uuid == transport.table_uuid && target.table_path == transport.table_path &&
	       target.table_schema_json == transport.table_schema_json && target.schema_id == transport.schema_id &&
	       target.append_only == transport.append_only && target.has_snapshot == transport.has_snapshot &&
	       target.snapshot_id == transport.snapshot_id && target.field_names == transport.input_names &&
	       target.part_keys == transport.part_keys && target.null_part_name == transport.null_part_name;
}

static bool RecoveryCommitIsVisible(PaimonCatalog &catalog, const paimon::Identifier &table_identifier,
                                    const PaimonDistributedInsertTransport &transport) {
	auto snapshots_result = catalog.GetPaimonCatalog().ListSnapshots(table_identifier);
	if (!snapshots_result.ok()) {
		throw IOException(snapshots_result.status().ToString());
	}
	const auto commit_user = "vane-" + CompactUUID(transport.operation_id);
	for (const auto &snapshot : snapshots_result.value()) {
		if (snapshot.commit_user == commit_user) {
			return true;
		}
	}
	return false;
}

static vector<PaimonDistributedRecoveryIntent> LoadRecoveryIntents(const paimon::Identifier &table_identifier,
                                                                   const string &table_path,
                                                                   const map<string, string> &options) {
	PaimonDistributedInsertTransport table_transport;
	table_transport.table_path = table_path;
	auto file_system = GetFileSystem(table_transport, options);
	const auto directory = RecoveryRoot(table_transport);
	std::vector<std::unique_ptr<paimon::BasicFileStatus>> entries;
	auto status = file_system->ListDir(directory, &entries);
	if (!status.ok()) {
		if (status.IsNotExist()) {
			return {};
		}
		throw IOException("Failed to list distributed Paimon recovery intents: %s", status.ToString());
	}
	vector<pair<string, string>> payloads;
	for (auto &entry : entries) {
		if (!entry || entry->IsDir() || entry->GetPath().empty()) {
			throw IOException("Distributed Paimon recovery directory contains an invalid entry");
		}
		const auto path = entry->GetPath();
		const auto name = FileName(path);
		if (!IsRecoveryIntentName(name)) {
			throw IOException("Distributed Paimon recovery directory contains an unknown artifact");
		}
		string payload;
		status = file_system->ReadFile(path, &payload);
		if (!status.ok()) {
			throw IOException("Failed to read distributed Paimon recovery intent: %s", status.ToString());
		}
		payloads.emplace_back(name, std::move(payload));
	}
	std::sort(payloads.begin(), payloads.end());
	vector<PaimonDistributedRecoveryIntent> result;
	for (auto &entry : payloads) {
		auto intent = DeserializeRecoveryIntent(entry.second);
		if (entry.first != RecoveryIntentName(intent.transport) || intent.transport.table_path != table_path ||
		    intent.transport.database_name != table_identifier.GetDatabaseName() ||
		    intent.transport.table_name != table_identifier.GetTableName()) {
			throw InvalidInputException("Distributed Paimon recovery intent does not match its table");
		}
		result.push_back(std::move(intent));
	}
	return result;
}

static void RecoverPendingOperations(PaimonCatalog &catalog, const paimon::Identifier &table_identifier,
                                     const string &table_path, const map<string, string> &base_options) {
	auto intents = LoadRecoveryIntents(table_identifier, table_path, base_options);
	for (const auto &intent : intents) {
		auto options = base_options;
		for (const auto &option : intent.transport.portable_options) {
			options[option.first] = option.second;
		}
		auto decoded = DecodeRecoveryIntent(intent);
		CloseOperation(intent.transport, options);
		if (intent.action == PaimonDistributedRecoveryAction::ABORT) {
			AbortMessages(intent.transport, options, "vane-" + CompactUUID(intent.transport.operation_id),
			              decoded.messages);
			CleanupAttemptManifests(intent.transport, options);
		} else if (RecoveryCommitIsVisible(catalog, table_identifier, intent.transport)) {
			CleanupAttemptManifests(intent.transport, options, &decoded.selected_attempt_manifests, false);
			DeleteSelectedAttemptManifests(intent.transport, options, decoded.selected_attempt_manifests);
		} else {
			auto target = LoadTargetState(catalog, table_identifier);
			if (!TargetMatchesRecoveryBaseline(target, intent.transport)) {
				AbortMessages(intent.transport, options, "vane-" + CompactUUID(intent.transport.operation_id),
				              decoded.messages);
				CleanupAttemptManifests(intent.transport, options);
			} else {
				CleanupAttemptManifests(intent.transport, options, &decoded.selected_attempt_manifests, false);
				auto committer = CreateCommitter(intent.transport, options,
				                                 "vane-" + CompactUUID(intent.transport.operation_id), true);
				std::map<int64_t, std::vector<std::shared_ptr<paimon::CommitMessage>>> commits;
				commits.emplace(intent.transport.commit_identifier, decoded.messages);
				auto commit_result = committer->FilterAndCommit(commits);
				if (!commit_result.ok()) {
					throw IOException("Failed to recover distributed Paimon INSERT commit: %s",
					                  commit_result.status().ToString());
				}
				if (commit_result.value() < 0 || commit_result.value() > 1) {
					throw IOException("Failed to recover distributed Paimon INSERT commit: invalid committed-operation "
					                  "count %d",
					                  commit_result.value());
				}
				DeleteSelectedAttemptManifests(intent.transport, options, decoded.selected_attempt_manifests);
			}
		}
		auto file_system = GetFileSystem(intent.transport, options);
		DeleteRecoveryIntent(*file_system, intent.transport);
	}
}

class PaimonDistributedInsertGlobalState final : public DistributedWriteGlobalState {
public:
	~PaimonDistributedInsertGlobalState() override {
		BestEffortAbortMessages(transport, options, writer_commit_user, messages);
	}

	PaimonDistributedInsertTransport transport;
	map<string, string> options;
	string query_id;
	string task_attempt_id;
	string writer_commit_user;
	std::atomic<uint32_t> next_write_id {0};
	mutex lock;
	std::vector<std::shared_ptr<paimon::CommitMessage>> messages;
	idx_t row_count = 0;
	vector<idx_t> part_col_idxs;
	bool finalized = false;
};

class PaimonDistributedInsertLocalState final : public DistributedWriteLocalState {
public:
	unique_ptr<paimon::FileStoreWrite> writer;
	idx_t row_count = 0;
};

static void ValidateTask(const PaimonDistributedInsertGlobalState &global, const DistributedWriteTaskContext &task) {
	task.Validate();
	(void)VaneLogicalTaskIdentity(task.query_id, task.task_attempt_id);
	if (global.query_id != task.query_id || global.task_attempt_id != task.task_attempt_id) {
		throw InvalidInputException("Distributed Paimon INSERT worker task identity changed during execution");
	}
}

static unique_ptr<DistributedWriteGlobalState>
PaimonDistributedInsertInitializeGlobal(ClientContext &context, const DistributedExtensionWriteInfo &info,
                                        const DistributedWriteTaskContext &task) {
	if (info.mode != DistributedWriteMode::CALLBACK || info.Name() != PAIMON_DISTRIBUTED_INSERT_OPERATOR ||
	    info.fragment_codec.name != PAIMON_DISTRIBUTED_INSERT_FRAGMENT_CODEC ||
	    info.fragment_codec.version != PAIMON_DISTRIBUTED_INSERT_PROTOCOL_VERSION) {
		throw InvalidInputException("Distributed Paimon INSERT worker contract does not match its registered protocol");
	}
	task.Validate();
	auto result = make_uniq<PaimonDistributedInsertGlobalState>();
	result->transport = DeserializeTransport(info.worker_bind_data);
	result->options = BuildRuntimeOptions(context, result->transport, &task);
	result->query_id = task.query_id;
	result->task_attempt_id = task.task_attempt_id;
	result->writer_commit_user = WorkerCommitUser(result->transport, task);
	for (const auto &part_key : result->transport.part_keys) {
		auto entry = std::find(result->transport.input_names.begin(), result->transport.input_names.end(), part_key);
		if (entry == result->transport.input_names.end()) {
			throw SerializationException("Distributed Paimon INSERT partition key '%s' is absent from worker input",
			                             part_key);
		}
		result->part_col_idxs.push_back(NumericCast<idx_t>(entry - result->transport.input_names.begin()));
	}
	return std::move(result);
}

static unique_ptr<DistributedWriteLocalState>
PaimonDistributedInsertInitializeLocal(ExecutionContext &, const DistributedExtensionWriteInfo &,
                                       const DistributedWriteTaskContext &task,
                                       DistributedWriteGlobalState &global_state) {
	auto &global = global_state.Cast<PaimonDistributedInsertGlobalState>();
	ValidateTask(global, task);
	auto write_id = global.next_write_id.fetch_add(1);
	if (write_id > static_cast<uint32_t>(NumericLimits<int32_t>::Maximum())) {
		throw InvalidInputException("Distributed Paimon INSERT worker exhausted its write identities");
	}
	paimon::WriteContextBuilder builder(global.transport.table_path, global.writer_commit_user);
	builder.WithWriteId(static_cast<int32_t>(write_id)).WithStreamingMode(true).SetOptions(global.options);
	auto context_result = builder.Finish();
	if (!context_result.ok()) {
		throw IOException(context_result.status().ToString());
	}
	auto writer_result = paimon::FileStoreWrite::Create(std::move(context_result).value());
	if (!writer_result.ok()) {
		throw IOException(writer_result.status().ToString());
	}
	auto result = make_uniq<PaimonDistributedInsertLocalState>();
	result->writer = unique_ptr<paimon::FileStoreWrite>(std::move(writer_result).value().release());
	return std::move(result);
}

static void WritePaimonChunk(PaimonDistributedInsertLocalState &local, DataChunk &chunk, ClientContext &client,
                             const std::map<string, string> &partition) {
	ArrowArrayWrapper arrow_wrapper;
	auto client_properties = client.GetClientProperties();
	client_properties.arrow_offset_size = ArrowOffsetSize::REGULAR;
	client_properties.arrow_use_list_view = false;
	client_properties.produce_arrow_string_view = false;
	client_properties.arrow_output_version = ArrowFormatVersion::V1_0;
	ArrowConverter::ToArrowArray(chunk, &arrow_wrapper.arrow_array, client_properties, {});
	paimon::RecordBatchBuilder builder(&arrow_wrapper.arrow_array);
	if (!partition.empty()) {
		builder.SetPartition(partition).SetBucket(0);
	}
	auto batch_result = builder.Finish();
	if (!batch_result.ok()) {
		throw IOException(batch_result.status().ToString());
	}
	auto status = local.writer->Write(std::move(batch_result).value());
	if (!status.ok()) {
		throw IOException(status.ToString());
	}
}

static void PaimonDistributedInsertSink(ExecutionContext &context, const DistributedExtensionWriteInfo &,
                                        const DistributedWriteTaskContext &task,
                                        DistributedWriteGlobalState &global_state,
                                        DistributedWriteLocalState &local_state, DataChunk &input) {
	auto &global = global_state.Cast<PaimonDistributedInsertGlobalState>();
	auto &local = local_state.Cast<PaimonDistributedInsertLocalState>();
	ValidateTask(global, task);
	if (input.GetTypes() != global.transport.input_types) {
		throw InvalidInputException("Distributed Paimon INSERT worker input schema does not match its frozen target");
	}
	if (global.part_col_idxs.empty()) {
		WritePaimonChunk(local, input, context.client, {});
		local.row_count = CheckedAdd(local.row_count, input.size(), "worker row count");
		return;
	}

	vector<UnifiedVectorFormat> part_formats(global.part_col_idxs.size());
	for (idx_t index = 0; index < global.part_col_idxs.size(); index++) {
		input.data[global.part_col_idxs[index]].ToUnifiedFormat(input.size(), part_formats[index]);
	}
	std::map<std::vector<string>, vector<idx_t>> groups;
	for (idx_t row = 0; row < input.size(); row++) {
		std::vector<string> key;
		key.reserve(global.part_col_idxs.size());
		for (idx_t index = 0; index < global.part_col_idxs.size(); index++) {
			auto source_index = part_formats[index].sel->get_index(row);
			if (!part_formats[index].validity.RowIsValid(source_index)) {
				key.push_back(global.transport.null_part_name);
			} else {
				key.push_back(input.GetValue(global.part_col_idxs[index], row).ToString());
			}
		}
		groups[std::move(key)].push_back(row);
	}
	for (auto &entry : groups) {
		std::map<string, string> partition;
		for (idx_t index = 0; index < global.transport.part_keys.size(); index++) {
			partition[global.transport.part_keys[index]] = entry.first[index];
		}
		SelectionVector selection(entry.second.size());
		for (idx_t index = 0; index < entry.second.size(); index++) {
			selection.set_index(index, entry.second[index]);
		}
		DataChunk slice;
		slice.Initialize(Allocator::DefaultAllocator(), input.GetTypes());
		slice.Slice(input, selection, entry.second.size());
		WritePaimonChunk(local, slice, context.client, partition);
	}
	local.row_count = CheckedAdd(local.row_count, input.size(), "worker row count");
}

static void PaimonDistributedInsertCombine(ExecutionContext &, const DistributedExtensionWriteInfo &,
                                           const DistributedWriteTaskContext &task,
                                           DistributedWriteGlobalState &global_state,
                                           DistributedWriteLocalState &local_state) {
	auto &global = global_state.Cast<PaimonDistributedInsertGlobalState>();
	auto &local = local_state.Cast<PaimonDistributedInsertLocalState>();
	ValidateTask(global, task);
	if (!local.writer) {
		throw InternalException("Distributed Paimon INSERT local writer was already combined");
	}
	if (local.row_count == 0) {
		auto status = local.writer->Close();
		local.writer.reset();
		if (!status.ok()) {
			throw IOException(status.ToString());
		}
		return;
	}

	auto messages_result = local.writer->PrepareCommit(false, global.transport.commit_identifier);
	if (!messages_result.ok()) {
		auto failure = messages_result.status().ToString();
		(void)local.writer->Close();
		local.writer.reset();
		throw IOException(failure);
	}
	auto messages = std::move(messages_result).value();
	auto close_status = local.writer->Close();
	local.writer.reset();
	if (!close_status.ok()) {
		BestEffortAbortMessages(global.transport, global.options, global.writer_commit_user, messages);
		throw IOException(close_status.ToString());
	}
	if (messages.empty()) {
		throw IOException("Distributed Paimon INSERT writer returned no commit messages for non-empty input");
	}
	for (const auto &message : messages) {
		if (!message) {
			std::vector<std::shared_ptr<paimon::CommitMessage>> valid_messages;
			for (const auto &candidate : messages) {
				if (candidate) {
					valid_messages.push_back(candidate);
				}
			}
			BestEffortAbortMessages(global.transport, global.options, global.writer_commit_user, valid_messages);
			throw IOException("Distributed Paimon INSERT writer returned an empty commit message");
		}
	}
	try {
		lock_guard<mutex> guard(global.lock);
		auto combined_row_count = CheckedAdd(global.row_count, local.row_count, "task row count");
		if (messages.size() > NumericLimits<idx_t>::Maximum() - global.messages.size()) {
			throw InvalidInputException("Distributed Paimon INSERT worker produced too many commit messages");
		}
		global.messages.reserve(global.messages.size() + messages.size());
		for (auto &message : messages) {
			global.messages.push_back(std::move(message));
		}
		global.row_count = combined_row_count;
	} catch (...) {
		BestEffortAbortMessages(global.transport, global.options, global.writer_commit_user, messages);
		throw;
	}
}

static vector<DistributedWriteFragment> PaimonDistributedInsertFinalize(ClientContext &,
                                                                        const DistributedExtensionWriteInfo &,
                                                                        const DistributedWriteTaskContext &task,
                                                                        DistributedWriteGlobalState &global_state) {
	auto &global = global_state.Cast<PaimonDistributedInsertGlobalState>();
	ValidateTask(global, task);
	if (global.finalized) {
		throw InvalidInputException("Distributed Paimon INSERT worker finalized more than once");
	}
	global.finalized = true;
	if (global.messages.empty()) {
		if (global.row_count != 0) {
			throw InternalException("Distributed Paimon INSERT lost commit messages for non-empty input");
		}
		return {};
	}
	if (global.row_count == 0) {
		throw InternalException("Distributed Paimon INSERT produced commit messages for empty input");
	}
	auto serialized_result = paimon::CommitMessage::SerializeList(global.messages, paimon::GetDefaultPool());
	if (!serialized_result.ok()) {
		BestEffortAbortMessages(global.transport, global.options, global.writer_commit_user, global.messages);
		throw IOException(serialized_result.status().ToString());
	}
	PaimonDistributedCommitEnvelope envelope;
	envelope.operation_id = global.transport.operation_id;
	envelope.table_uuid = global.transport.table_uuid;
	envelope.table_path = global.transport.table_path;
	envelope.schema_fingerprint = SchemaFingerprint(global.transport.table_schema_json);
	envelope.schema_id = global.transport.schema_id;
	envelope.has_snapshot = global.transport.has_snapshot;
	envelope.snapshot_id = global.transport.snapshot_id;
	envelope.commit_identifier = global.transport.commit_identifier;
	envelope.query_id = task.query_id;
	envelope.task_attempt_id = task.task_attempt_id;
	envelope.writer_commit_user = global.writer_commit_user;
	envelope.commit_message_version = paimon::CommitMessage::CurrentVersion();
	envelope.message_count = global.messages.size();
	envelope.row_count = global.row_count;
	envelope.serialized_messages = std::move(serialized_result).value();
	auto manifest_payload = SerializeCommitEnvelope(envelope);
	DistributedWriteFragment fragment;
	fragment.fragment_id = ExpectedFragmentId(global.transport, task.task_attempt_id);
	fragment.payload = std::move(manifest_payload);
	fragment.row_count = global.row_count;
	fragment.byte_count = 0;
	DistributedWriteArtifact manifest_artifact;
	manifest_artifact.artifact_id = PAIMON_DISTRIBUTED_ATTEMPT_MANIFEST_ARTIFACT;
	manifest_artifact.uri = AttemptManifestPath(global.transport, task);
	manifest_artifact.codec = {PAIMON_DISTRIBUTED_ATTEMPT_MANIFEST_CODEC, PAIMON_DISTRIBUTED_INSERT_PROTOCOL_VERSION};
	fragment.artifacts.push_back(std::move(manifest_artifact));
	vector<DistributedWriteFragment> fragments;
	fragments.push_back(std::move(fragment));
	// Publish recovery metadata only after the complete task envelope is immutable. The manifest assumes ownership of
	// cleanup when Vane discards a successful retry/speculative result; until publication succeeds, the global-state
	// destructor remains armed and aborts every prepared data/index file.
	WriteAttemptManifest(global.transport, global.options, task, global.writer_commit_user, global.messages,
	                     fragments[0].payload);
	global.messages.clear();
	return fragments;
}

static DistributedExtensionWriteCallbacks PaimonDistributedInsertCallbacks() {
	DistributedExtensionWriteCallbacks callbacks;
	callbacks.initialize_global = PaimonDistributedInsertInitializeGlobal;
	callbacks.initialize_local = PaimonDistributedInsertInitializeLocal;
	callbacks.sink = PaimonDistributedInsertSink;
	callbacks.combine = PaimonDistributedInsertCombine;
	callbacks.finalize = PaimonDistributedInsertFinalize;
	return callbacks;
}

static void ValidateResolvedInfo(const DistributedExtensionWriteInfo &info) {
	if (info.mode != DistributedWriteMode::CALLBACK || info.capability.extension_name != "paimon" ||
	    info.capability.capability.name != PAIMON_DISTRIBUTED_INSERT_OPERATOR ||
	    info.capability.capability.protocol_version != PAIMON_DISTRIBUTED_INSERT_PROTOCOL_VERSION ||
	    info.fragment_codec.name != PAIMON_DISTRIBUTED_INSERT_FRAGMENT_CODEC ||
	    info.fragment_codec.version != PAIMON_DISTRIBUTED_INSERT_PROTOCOL_VERSION) {
		throw InvalidInputException("Distributed Paimon INSERT coordinator contract does not match its registration");
	}
}

static void DecodeCommitResults(const DistributedExtensionWriteInfo &info,
                                const PaimonDistributedInsertTransport &transport,
                                const vector<DistributedWriteTaskResult> &results,
                                PaimonDecodedDistributedCommit &decoded) {
	ValidateResolvedInfo(info);
	unordered_set<string> task_attempt_ids;
	unordered_set<string> logical_task_ids;
	unordered_set<string> attempt_manifest_names;
	unordered_set<string> fragment_ids;
	unordered_set<string> canonical_messages;
	string query_id;
	string duplicate_logical_task_id;
	for (const auto &result : results) {
		result.Validate();
		if (result.capability != info.capability || result.fragment_codec != info.fragment_codec) {
			throw InvalidInputException(
			    "Distributed Paimon INSERT task result does not match its coordinator contract");
		}
		if (query_id.empty()) {
			query_id = result.query_id;
		} else if (query_id != result.query_id) {
			throw InvalidInputException(
			    "Distributed Paimon INSERT selected results have inconsistent query identities");
		}
		if (!task_attempt_ids.insert(result.task_attempt_id).second) {
			throw InvalidInputException("Distributed Paimon INSERT selected task attempt '%s' more than once",
			                            result.task_attempt_id);
		}
		auto logical_task_id = VaneLogicalTaskIdentity(result.query_id, result.task_attempt_id);
		if (!logical_task_ids.insert(logical_task_id).second && duplicate_logical_task_id.empty()) {
			duplicate_logical_task_id = std::move(logical_task_id);
		}
		DistributedWriteTaskContext task {result.query_id, result.task_attempt_id};
		const auto manifest_name = AttemptManifestName(task);
		if (!attempt_manifest_names.insert(manifest_name).second) {
			throw InvalidInputException("Distributed Paimon INSERT selected attempts have colliding namespaces");
		}
		if (result.fragments.empty()) {
			if (result.RowCount() != 0 || result.ByteCount() != 0) {
				throw InvalidInputException("Empty distributed Paimon INSERT task result has non-zero counts");
			}
			continue;
		}
		if (result.fragments.size() != 1) {
			throw InvalidInputException(
			    "Distributed Paimon INSERT task result must contain at most one commit fragment");
		}
		const auto &fragment = result.fragments[0];
		if (fragment.fragment_id != ExpectedFragmentId(transport, result.task_attempt_id) ||
		    !fragment_ids.insert(fragment.fragment_id).second || fragment.artifacts.size() != 1 ||
		    fragment.byte_count != 0) {
			throw InvalidInputException("Distributed Paimon INSERT task result has invalid fragment metadata");
		}
		const auto &manifest_artifact = fragment.artifacts[0];
		if (manifest_artifact.artifact_id != PAIMON_DISTRIBUTED_ATTEMPT_MANIFEST_ARTIFACT ||
		    manifest_artifact.uri != AttemptManifestPath(transport, task) ||
		    manifest_artifact.codec != DistributedPayloadCodec {PAIMON_DISTRIBUTED_ATTEMPT_MANIFEST_CODEC,
		                                                        PAIMON_DISTRIBUTED_INSERT_PROTOCOL_VERSION} ||
		    !manifest_artifact.payload.empty() ||
		    !decoded.selected_attempt_manifests.emplace(manifest_name, optional_ptr<const string>(&fragment.payload))
		         .second) {
			throw InvalidInputException("Distributed Paimon INSERT task result has an invalid attempt manifest");
		}
		auto envelope = DeserializeCommitEnvelope(fragment.payload);
		ValidateAttemptManifestEnvelope(transport, envelope, manifest_name);
		if (envelope.query_id != result.query_id || envelope.task_attempt_id != result.task_attempt_id ||
		    envelope.row_count != fragment.row_count || envelope.row_count != result.RowCount()) {
			throw InvalidInputException(
			    "Distributed Paimon INSERT commit fragment does not match its target or attempt");
		}
		if (envelope.serialized_messages.size() > static_cast<idx_t>(NumericLimits<int32_t>::Maximum())) {
			throw InvalidInputException("Distributed Paimon INSERT commit fragment exceeds the Paimon codec limit");
		}
		auto messages_result = paimon::CommitMessage::DeserializeList(
		    envelope.commit_message_version, envelope.serialized_messages.data(),
		    NumericCast<int32_t>(envelope.serialized_messages.size()), paimon::GetDefaultPool());
		if (!messages_result.ok()) {
			throw InvalidInputException("Failed to deserialize distributed Paimon commit messages: %s",
			                            messages_result.status().ToString());
		}
		auto messages = std::move(messages_result).value();
		if (messages.size() != envelope.message_count || messages.empty()) {
			throw InvalidInputException("Distributed Paimon INSERT commit-message count does not match its envelope");
		}
		for (auto &message : messages) {
			if (!message) {
				throw InvalidInputException("Distributed Paimon INSERT commit fragment contains an empty message");
			}
			auto canonical_result = paimon::CommitMessage::Serialize(message, paimon::GetDefaultPool());
			if (!canonical_result.ok()) {
				throw InvalidInputException("Failed to canonicalize distributed Paimon commit message: %s",
				                            canonical_result.status().ToString());
			}
			if (!canonical_messages.insert(std::move(canonical_result).value()).second) {
				throw InvalidInputException("Distributed Paimon INSERT selected a duplicate commit message");
			}
			decoded.messages.push_back(std::move(message));
		}
		decoded.row_count = CheckedAdd(decoded.row_count, envelope.row_count, "selected row count");
	}
	if (!duplicate_logical_task_id.empty()) {
		throw InvalidInputException("Distributed Paimon INSERT selected multiple attempts for Vane logical task '%s'",
		                            duplicate_logical_task_id);
	}
}

static PaimonDistributedInsertTransport GetCoordinatorTransport(const PhysicalPaimonInsert &insert) {
	auto transport = DeserializeTransport(insert.distributed_write_plan.worker_bind_data);
	if (transport.operation_id != insert.distributed_operation_id ||
	    transport.database_name != insert.table_identifier.GetDatabaseName() ||
	    transport.table_name != insert.table_identifier.GetTableName() ||
	    transport.table_uuid != insert.distributed_table_uuid ||
	    transport.table_path != insert.distributed_table_path ||
	    transport.table_schema_json != insert.distributed_table_schema_json ||
	    transport.schema_id != insert.distributed_schema_id ||
	    transport.append_only != insert.distributed_append_only ||
	    transport.has_snapshot != insert.distributed_has_snapshot ||
	    transport.snapshot_id != insert.distributed_snapshot_id ||
	    transport.commit_identifier != insert.distributed_commit_identifier ||
	    transport.input_types != insert.distributed_input_types ||
	    transport.input_names != insert.distributed_input_names || transport.part_keys != insert.part_keys ||
	    transport.null_part_name != insert.distributed_null_part_name ||
	    transport.portable_options != insert.distributed_portable_options) {
		throw InvalidInputException("Distributed Paimon INSERT worker bind does not match its coordinator target");
	}
	return transport;
}

static void ValidateCoordinatorShape(const PhysicalPaimonInsert &insert) {
	if (insert.info) {
		throw NotImplementedException("Distributed Paimon CTAS is not implemented by the INSERT protocol");
	}
	if (!insert.distributed_append_only) {
		throw NotImplementedException("Distributed Paimon INSERT supports append-only tables");
	}
	if (!insert.distributed_target_initialized || !insert.distributed_worker_plan_selected ||
	    !insert.distributed_operation_open || insert.distributed_operation_options.empty() ||
	    insert.distributed_operation_path != insert.distributed_table_path ||
	    insert.distributed_write_plan.extension_name != "paimon" ||
	    insert.distributed_write_plan.operator_name != PAIMON_DISTRIBUTED_INSERT_OPERATOR ||
	    insert.distributed_write_plan.worker_bind_data.empty() || insert.children.size() != 1 ||
	    insert.children[0].get().types != insert.distributed_input_types) {
		throw InvalidInputException("Distributed Paimon INSERT target or worker plan was not initialized");
	}
	(void)GetCoordinatorTransport(insert);
}

static void ValidateTargetBaseline(ClientContext &, const PhysicalPaimonInsert &insert) {
	if (!insert.schema) {
		throw InvalidInputException("Distributed Paimon INSERT has no coordinator schema entry");
	}
	auto &catalog = insert.schema->catalog.Cast<PaimonCatalog>();
	if (catalog.GetAccessMode() == AccessMode::READ_ONLY) {
		throw PermissionException("Cannot write to a read-only Paimon catalog");
	}
	auto current = LoadTargetState(catalog, insert.table_identifier);
	if (current.table_uuid != insert.distributed_table_uuid) {
		throw TransactionException("Paimon table %s was replaced after the distributed INSERT was planned",
		                           insert.table_identifier.ToString());
	}
	if (current.table_path != insert.distributed_table_path) {
		throw TransactionException("Paimon table %s path changed after the distributed INSERT was planned",
		                           insert.table_identifier.ToString());
	}
	if (current.schema_id != insert.distributed_schema_id ||
	    current.table_schema_json != insert.distributed_table_schema_json ||
	    current.append_only != insert.distributed_append_only ||
	    current.field_names != insert.distributed_input_names || current.part_keys != insert.part_keys ||
	    current.null_part_name != insert.distributed_null_part_name) {
		throw TransactionException("Paimon table %s schema changed after the distributed INSERT was planned",
		                           insert.table_identifier.ToString());
	}
	if (current.has_snapshot != insert.distributed_has_snapshot ||
	    current.snapshot_id != insert.distributed_snapshot_id) {
		throw TransactionException("Paimon table %s snapshot changed after the distributed INSERT was planned",
		                           insert.table_identifier.ToString());
	}
}

} // namespace

PhysicalPaimonInsert::~PhysicalPaimonInsert() {
	if (!distributed_operation_open || distributed_write_plan.worker_bind_data.empty()) {
		return;
	}
	// Vane can reject a plan before workers run and before the abort hook is armed. Closing here makes the marker's
	// lifetime no longer than the coordinator plan that could authorize those workers.
	try {
		auto transport = DeserializeTransport(distributed_write_plan.worker_bind_data);
		if (BestEffortCloseOperation(transport, distributed_operation_options)) {
			distributed_operation_open = false;
			if (!distributed_recovery_intent_published) {
				BestEffortCleanupAttemptManifests(transport, distributed_operation_options);
			}
		}
	} catch (...) {
	}
}

void PhysicalPaimonInsert::InitializeDistributedWrite(const vector<LogicalType> &input_types) {
	if (info) {
		return;
	}
	if (distributed_target_initialized) {
		throw InternalException("Distributed Paimon INSERT target was initialized more than once");
	}
	if (!schema) {
		throw InternalException("Distributed Paimon INSERT requires a coordinator schema entry");
	}
	if (distributed_operation_path.empty() || distributed_operation_options.empty()) {
		throw InternalException("Distributed Paimon INSERT requires its table file-system options");
	}
	auto &catalog = schema->catalog.Cast<PaimonCatalog>();
	auto vane_mutex = catalog.GetVaneCatalogMutationMutex();
	std::unique_lock<std::mutex> vane_guard(*vane_mutex);
	RecoverPendingOperations(catalog, table_identifier, distributed_operation_path, distributed_operation_options);
	auto target = LoadTargetState(catalog, table_identifier);
	if (target.table_path != distributed_operation_path) {
		throw TransactionException("Paimon table %s path changed after its file-system options were resolved",
		                           table_identifier.ToString());
	}
	if (input_types.size() != target.field_names.size()) {
		throw InvalidInputException("Paimon INSERT input schema does not match the target table width");
	}
	for (const auto &type : input_types) {
		if (type.id() == LogicalTypeId::INVALID) {
			throw InvalidInputException("Paimon INSERT input schema contains an invalid type");
		}
	}
	if (part_keys != target.part_keys) {
		throw InternalException("Paimon INSERT partition keys do not match the loaded target schema");
	}
	distributed_operation_id = UUID::ToString(UUID::GenerateRandomUUID());
	distributed_commit_identifier = CreateCommitIdentifier(distributed_operation_id);
	distributed_table_uuid = std::move(target.table_uuid);
	distributed_table_path = std::move(target.table_path);
	distributed_table_schema_json = std::move(target.table_schema_json);
	distributed_schema_id = target.schema_id;
	distributed_append_only = target.append_only;
	distributed_has_snapshot = target.has_snapshot;
	distributed_snapshot_id = target.snapshot_id;
	distributed_input_types = input_types;
	distributed_input_names = std::move(target.field_names);
	distributed_null_part_name = std::move(target.null_part_name);
	distributed_portable_options = GetPortableWriteOptions(paimon_options);
	for (const auto &option : distributed_portable_options) {
		distributed_operation_options[option.first] = option.second;
	}

	PaimonDistributedInsertTransport transport;
	transport.operation_id = distributed_operation_id;
	transport.database_name = table_identifier.GetDatabaseName();
	transport.table_name = table_identifier.GetTableName();
	transport.table_uuid = distributed_table_uuid;
	transport.table_path = distributed_table_path;
	transport.table_schema_json = distributed_table_schema_json;
	transport.schema_id = distributed_schema_id;
	transport.append_only = distributed_append_only;
	transport.has_snapshot = distributed_has_snapshot;
	transport.snapshot_id = distributed_snapshot_id;
	transport.commit_identifier = distributed_commit_identifier;
	transport.input_types = distributed_input_types;
	transport.input_names = distributed_input_names;
	transport.part_keys = part_keys;
	transport.null_part_name = distributed_null_part_name;
	transport.portable_options = distributed_portable_options;
	auto worker_bind_data = SerializeTransport(transport);
	try {
		// The marker is the worker's positive authorization to publish. It must exist before Vane can serialize the
		// worker plan; removing it later closes the operation without retaining a tombstone for every INSERT.
		OpenOperation(transport, distributed_operation_options);
		distributed_write_plan.extension_name = "paimon";
		distributed_write_plan.operator_name = PAIMON_DISTRIBUTED_INSERT_OPERATOR;
		distributed_write_plan.worker_bind_data = std::move(worker_bind_data);
		distributed_target_initialized = true;
		distributed_operation_open = true;
	} catch (...) {
		(void)BestEffortCloseOperation(transport, distributed_operation_options);
		throw;
	}
}

optional_ptr<distributed::ExtensionWriteTaskProvider> PhysicalPaimonInsert::GetExtensionWriteTaskProvider() {
	if (info) {
		throw NotImplementedException("Distributed Paimon CTAS is tracked separately from distributed INSERT");
	}
	if (distributed_worker_plan_selected) {
		return this;
	}
	if (children.size() != 1) {
		throw InvalidInputException("Distributed Paimon INSERT requires exactly one physical child");
	}
	if (!distributed_target_initialized) {
		InitializeDistributedWrite(children[0].get().types);
	}
	distributed_worker_plan_selected = true;
	return this;
}

const distributed::DistributedExtensionWritePlan &PhysicalPaimonInsert::WritePlan() const {
	ValidateCoordinatorShape(*this);
	return distributed_write_plan;
}

void PhysicalPaimonInsert::ValidateDistributedWrite(ClientContext &context) const {
	ValidateCoordinatorShape(*this);
	ValidateTargetBaseline(context, *this);
}

idx_t PhysicalPaimonInsert::FinalizeDistributedWrite(ClientContext &context,
                                                     const vector<DistributedWriteTaskResult> &results) const {
	ValidateCoordinatorShape(*this);
	if (distributed_finalize_started) {
		throw InvalidInputException("Distributed Paimon INSERT coordinator finalized more than once");
	}
	distributed_finalize_started = true;
	auto transport = GetCoordinatorTransport(*this);
	map<string, string> options;
	try {
		options = BuildRuntimeOptions(context, transport);
	} catch (...) {
		if (BestEffortCloseOperation(transport, distributed_operation_options)) {
			distributed_operation_open = false;
			BestEffortCleanupAttemptManifests(transport, distributed_operation_options);
		}
		throw;
	}
	const auto coordinator_commit_user = "vane-" + CompactUUID(transport.operation_id);
	PaimonDecodedDistributedCommit decoded;
	try {
		auto resolved_info = distributed::ResolveDistributedExtensionWriteInfo(context, distributed_write_plan);
		DecodeCommitResults(resolved_info, transport, results, decoded);
		if (decoded.messages.empty()) {
			if (decoded.row_count != 0) {
				throw InvalidInputException("Distributed Paimon INSERT returned rows without commit messages");
			}
		} else if (decoded.row_count == 0) {
			throw InvalidInputException("Distributed Paimon INSERT returned commit messages without rows");
		}
	} catch (...) {
		if (BestEffortCloseOperation(transport, options)) {
			distributed_operation_open = false;
		}
		BestEffortAbortMessages(transport, options, coordinator_commit_user, decoded.messages);
		BestEffortCleanupAttemptManifests(transport, options);
		throw;
	}

	PaimonDistributedRecoveryIntent recovery_intent;
	string recovery_payload;
	try {
		recovery_intent.transport = transport;
		recovery_intent.action =
		    decoded.messages.empty() ? PaimonDistributedRecoveryAction::ABORT : PaimonDistributedRecoveryAction::COMMIT;
		recovery_intent.selected_attempt_manifests = CopySelectedAttemptManifests(decoded.selected_attempt_manifests);
		recovery_intent.row_count = decoded.row_count;
		recovery_payload = SerializeRecoveryIntent(recovery_intent);
	} catch (...) {
		if (BestEffortCloseOperation(transport, options)) {
			distributed_operation_open = false;
		}
		BestEffortAbortMessages(transport, options, coordinator_commit_user, decoded.messages);
		BestEffortCleanupAttemptManifests(transport, options);
		throw;
	}
	try {
		auto &catalog = schema->catalog.Cast<PaimonCatalog>();
		auto vane_mutex = catalog.GetVaneCatalogMutationMutex();
		std::unique_lock<std::mutex> vane_guard(*vane_mutex);
		// The process-wide warehouse lock serializes target validation, durable intent publication, commit, and
		// cleanup. A later INSERT acquires the same lock and resolves any intent left by process termination.
		ValidateTargetBaseline(context, *this);
		bool recovery_intent_may_exist = false;
		try {
			PublishRecoveryIntent(recovery_intent, recovery_payload, options, recovery_intent_may_exist);
			distributed_recovery_intent_published = true;
		} catch (...) {
			// On an ambiguous store error, retaining prepared files is safer than aborting data that a recovery record
			// may still commit. A confirmed-absent intent remains eligible for ordinary abort cleanup.
			distributed_recovery_intent_published = recovery_intent_may_exist;
			throw;
		}
		// Removing the positive-authorization marker after the intent is durable fences late attempts without opening
		// a crash window in which prepared files have neither an authorization marker nor recoverable coordinator
		// state.
		CloseOperation(transport, options);
		distributed_operation_open = false;
		if (recovery_intent.action == PaimonDistributedRecoveryAction::ABORT) {
			AbortMessages(transport, options, coordinator_commit_user, decoded.messages);
			CleanupAttemptManifests(transport, options);
			if (BestEffortDeleteRecoveryIntent(transport, options)) {
				distributed_recovery_intent_published = false;
			}
			return 0;
		}
		// Vane forwards only selected task results. The operation-scoped manifest directory supplies commit messages
		// for completed retry/speculative losers. The intent owns exact copies of the selected manifests, so recovery
		// can proceed even if one of their filesystem copies disappeared after publication.
		CleanupAttemptManifests(transport, options, &decoded.selected_attempt_manifests, false);
		auto committer = CreateCommitter(transport, options, coordinator_commit_user, true);
		std::map<int64_t, std::vector<std::shared_ptr<paimon::CommitMessage>>> commits;
		commits.emplace(transport.commit_identifier, decoded.messages);
		auto commit_result = committer->FilterAndCommit(commits);
		if (!commit_result.ok()) {
			throw IOException("Distributed Paimon INSERT commit outcome is unknown: %s",
			                  commit_result.status().ToString());
		}
		if (commit_result.value() < 0 || commit_result.value() > 1) {
			throw IOException(
			    "Distributed Paimon INSERT commit outcome is unknown: expected at most one committed operation, got %d",
			    commit_result.value());
		}
		// Delete the recovery intent only after every operation-scoped artifact is confirmed absent. A failed cleanup
		// leaves the intent in place and does not turn a successful catalog commit into a user-visible failure.
		if (BestEffortDeleteSelectedAttemptManifests(transport, options, decoded.selected_attempt_manifests) &&
		    BestEffortDeleteRecoveryIntent(transport, options)) {
			distributed_recovery_intent_published = false;
		}
	} catch (...) {
		if (!distributed_recovery_intent_published) {
			if (BestEffortCloseOperation(transport, options)) {
				distributed_operation_open = false;
			}
			BestEffortAbortMessages(transport, options, coordinator_commit_user, decoded.messages);
			BestEffortCleanupAttemptManifests(transport, options);
		}
		throw;
	}
	return decoded.row_count;
}

void PhysicalPaimonInsert::AbortDistributedWrite(ClientContext &context,
                                                 const vector<DistributedWriteTaskResult> &selected_results) const {
	ValidateCoordinatorShape(*this);
	if (distributed_finalize_started) {
		throw InvalidInputException("Distributed Paimon INSERT cannot abort after coordinator finalization started");
	}
	auto transport = GetCoordinatorTransport(*this);
	map<string, string> options;
	try {
		options = BuildRuntimeOptions(context, transport);
	} catch (...) {
		if (BestEffortCloseOperation(transport, distributed_operation_options)) {
			distributed_operation_open = false;
			BestEffortCleanupAttemptManifests(transport, distributed_operation_options);
		}
		throw;
	}
	PaimonDecodedDistributedCommit decoded;
	std::exception_ptr decode_error;
	try {
		auto resolved_info = distributed::ResolveDistributedExtensionWriteInfo(context, distributed_write_plan);
		DecodeCommitResults(resolved_info, transport, selected_results, decoded);
	} catch (...) {
		decode_error = std::current_exception();
	}
	PaimonDistributedRecoveryIntent recovery_intent;
	string recovery_payload;
	try {
		recovery_intent.transport = transport;
		recovery_intent.action = PaimonDistributedRecoveryAction::ABORT;
		if (!decode_error) {
			recovery_intent.selected_attempt_manifests =
			    CopySelectedAttemptManifests(decoded.selected_attempt_manifests);
		} else {
			recovery_intent.selected_attempt_manifests.clear();
		}
		recovery_payload = SerializeRecoveryIntent(recovery_intent);
	} catch (...) {
		if (BestEffortCloseOperation(transport, options)) {
			distributed_operation_open = false;
		}
		BestEffortAbortMessages(transport, options, "vane-" + CompactUUID(transport.operation_id), decoded.messages);
		BestEffortCleanupAttemptManifests(transport, options);
		throw;
	}
	try {
		auto &catalog = schema->catalog.Cast<PaimonCatalog>();
		auto vane_mutex = catalog.GetVaneCatalogMutationMutex();
		std::unique_lock<std::mutex> vane_guard(*vane_mutex);
		bool recovery_intent_may_exist = false;
		try {
			PublishRecoveryIntent(recovery_intent, recovery_payload, options, recovery_intent_may_exist);
			distributed_recovery_intent_published = true;
		} catch (...) {
			distributed_recovery_intent_published = recovery_intent_may_exist;
			throw;
		}
		CloseOperation(transport, options);
		distributed_operation_open = false;
		AbortMessages(transport, options, "vane-" + CompactUUID(transport.operation_id), decoded.messages);
		CleanupAttemptManifests(transport, options);
		if (BestEffortDeleteRecoveryIntent(transport, options)) {
			distributed_recovery_intent_published = false;
		}
	} catch (...) {
		if (!distributed_recovery_intent_published) {
			if (BestEffortCloseOperation(transport, options)) {
				distributed_operation_open = false;
			}
			BestEffortAbortMessages(transport, options, "vane-" + CompactUUID(transport.operation_id),
			                        decoded.messages);
			BestEffortCleanupAttemptManifests(transport, options);
		}
		throw;
	}
	if (decode_error) {
		std::rethrow_exception(decode_error);
	}
}

void PhysicalPaimonInsert::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	if (distributed_worker_plan_selected) {
		throw InvalidInputException(
		    "A distributed Paimon INSERT worker plan cannot execute as a native coordinator operator");
	}
	PhysicalOperator::BuildPipelines(current, meta_pipeline);
}

void RegisterPaimonDistributedWrites(ExtensionLoader &loader) {
	DistributedWriteOperatorExtension extension;
	extension.name = PAIMON_DISTRIBUTED_INSERT_OPERATOR;
	extension.protocol_version = PAIMON_DISTRIBUTED_INSERT_PROTOCOL_VERSION;
	extension.mode = DistributedWriteMode::CALLBACK;
	extension.fragment_codec = {PAIMON_DISTRIBUTED_INSERT_FRAGMENT_CODEC, PAIMON_DISTRIBUTED_INSERT_PROTOCOL_VERSION};
	extension.callbacks = PaimonDistributedInsertCallbacks();
	DistributedWriteOperatorExtension::Register(loader, std::move(extension));
}

} // namespace duckdb
