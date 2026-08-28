/*-------------------------------------------------------------------------
 *
 * paimon_distributed_scan.cpp
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
 *	  src/paimon_storage/paimon_distributed_scan.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "paimon_scan.hpp"

#include "paimon_catalog.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/function/distributed_table_function.hpp"

#include "paimon/defs.h"
#include "paimon/global_index/indexed_split.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/table/source/data_split.h"
#include "paimon/table/source/plan.h"
#include "paimon/table/source/split.h"

namespace duckdb {

namespace {

static constexpr uint32_t PAIMON_DISTRIBUTED_SCAN_PROTOCOL_VERSION = 1;
static constexpr const char *PAIMON_DISTRIBUTED_SCAN_SPLIT_CODEC = "paimon.scan-split";

enum class PaimonDistributedBindKind : uint8_t { COORDINATOR = 1, WORKER = 2 };

struct PaimonDistributedSplitEnvelope {
	string split_set_id;
	string split_id;
	bool has_snapshot = false;
	int64_t snapshot_id = 0;
	int64_t schema_id = -1;
	string paimon_split;
	bool has_estimated_cardinality = false;
	idx_t estimated_cardinality = 0;
	bool has_estimated_bytes = false;
	idx_t estimated_bytes = 0;
};

struct PaimonDistributedBindTransport {
	PaimonTablePath path;
	string table_data_path;
	string table_schema_json;
	int64_t schema_id = -1;
	bool append_only = false;
	vector<LogicalType> return_types;
	vector<string> return_names;
	vector<string> part_keys;
	map<string, string> portable_options;
	string split_set_id;
};

struct PaimonDistributedSplitEstimates {
	bool has_cardinality = false;
	idx_t cardinality = 0;
	bool has_bytes = false;
	idx_t bytes = 0;
};

static bool IsCanonicalSplitId(const string &split_id) {
	if (split_id.empty() || (split_id.size() > 1 && split_id[0] == '0')) {
		return false;
	}
	for (auto character : split_id) {
		if (character < '0' || character > '9') {
			return false;
		}
	}
	return true;
}

static bool TryAddEstimate(int64_t value, idx_t &total) {
	if (value < 0) {
		return false;
	}
	auto unsigned_value = static_cast<uint64_t>(value);
	auto maximum = static_cast<uint64_t>(NumericLimits<idx_t>::Maximum());
	if (unsigned_value >= maximum || total >= NumericLimits<idx_t>::Maximum() - unsigned_value) {
		return false;
	}
	total += NumericCast<idx_t>(unsigned_value);
	return true;
}

static PaimonDistributedSplitEstimates GetSplitEstimates(const std::shared_ptr<paimon::Split> &split,
                                                         bool append_only) {
	PaimonDistributedSplitEstimates result;
	auto data_split = std::dynamic_pointer_cast<paimon::DataSplit>(split);
	if (!data_split) {
		auto indexed_split = std::dynamic_pointer_cast<paimon::IndexedSplit>(split);
		if (indexed_split) {
			data_split = indexed_split->GetDataSplit();
		}
	}
	if (!data_split) {
		return result;
	}

	idx_t bytes = 0;
	idx_t cardinality = 0;
	bool valid_bytes = true;
	bool valid_cardinality = append_only;
	for (const auto &file : data_split->GetFileList()) {
		if (valid_bytes && !TryAddEstimate(file.file_size, bytes)) {
			valid_bytes = false;
		}
		if (valid_cardinality && !TryAddEstimate(file.row_count, cardinality)) {
			valid_cardinality = false;
		}
	}
	result.has_bytes = valid_bytes;
	result.bytes = bytes;
	// Indexed splits describe a subset of the underlying data split. Its file
	// row count is therefore not a valid estimate for the assigned work.
	result.has_cardinality = valid_cardinality && !std::dynamic_pointer_cast<paimon::IndexedSplit>(split);
	result.cardinality = cardinality;
	return result;
}

static string SerializeSplitEnvelope(const PaimonDistributedSplitEnvelope &envelope) {
	MemoryStream stream(Allocator::DefaultAllocator());
	BinarySerializer serializer(stream);
	serializer.Begin();
	serializer.WriteProperty(1, "protocol_version", PAIMON_DISTRIBUTED_SCAN_PROTOCOL_VERSION);
	serializer.WriteProperty(2, "split_set_id", envelope.split_set_id);
	serializer.WriteProperty(3, "split_id", envelope.split_id);
	serializer.WriteProperty(4, "has_snapshot", envelope.has_snapshot);
	serializer.WriteProperty(5, "snapshot_id", envelope.has_snapshot ? envelope.snapshot_id : 0);
	serializer.WriteProperty(6, "schema_id", envelope.schema_id);
	serializer.WriteProperty(7, "paimon_split", envelope.paimon_split);
	serializer.WriteProperty(8, "has_estimated_cardinality", envelope.has_estimated_cardinality);
	serializer.WriteProperty(9, "estimated_cardinality",
	                         envelope.has_estimated_cardinality ? envelope.estimated_cardinality : 0);
	serializer.WriteProperty(10, "has_estimated_bytes", envelope.has_estimated_bytes);
	serializer.WriteProperty(11, "estimated_bytes", envelope.has_estimated_bytes ? envelope.estimated_bytes : 0);
	serializer.End();
	return string(reinterpret_cast<const char *>(stream.GetData()), stream.GetPosition());
}

static PaimonDistributedSplitEnvelope DeserializeSplitEnvelope(const string &payload) {
	if (payload.empty()) {
		throw SerializationException("Cannot deserialize an empty distributed Paimon scan split");
	}
	vector<data_t> buffer(payload.begin(), payload.end());
	MemoryStream stream(buffer.data(), buffer.size());
	BinaryDeserializer deserializer(stream);
	deserializer.Begin();
	auto protocol_version = deserializer.ReadProperty<uint32_t>(1, "protocol_version");
	if (protocol_version != PAIMON_DISTRIBUTED_SCAN_PROTOCOL_VERSION) {
		throw SerializationException("Distributed Paimon scan split has unsupported protocol version %u",
		                             protocol_version);
	}
	PaimonDistributedSplitEnvelope result;
	result.split_set_id = deserializer.ReadProperty<string>(2, "split_set_id");
	result.split_id = deserializer.ReadProperty<string>(3, "split_id");
	result.has_snapshot = deserializer.ReadProperty<bool>(4, "has_snapshot");
	result.snapshot_id = deserializer.ReadProperty<int64_t>(5, "snapshot_id");
	result.schema_id = deserializer.ReadProperty<int64_t>(6, "schema_id");
	result.paimon_split = deserializer.ReadProperty<string>(7, "paimon_split");
	result.has_estimated_cardinality = deserializer.ReadProperty<bool>(8, "has_estimated_cardinality");
	result.estimated_cardinality = deserializer.ReadProperty<idx_t>(9, "estimated_cardinality");
	result.has_estimated_bytes = deserializer.ReadProperty<bool>(10, "has_estimated_bytes");
	result.estimated_bytes = deserializer.ReadProperty<idx_t>(11, "estimated_bytes");
	deserializer.End();

	if (result.split_set_id.empty() || !IsCanonicalSplitId(result.split_id) || result.schema_id < 0 ||
	    (result.has_snapshot && result.snapshot_id < 0) || result.paimon_split.empty() ||
	    (result.has_estimated_cardinality && result.estimated_cardinality == NumericLimits<idx_t>::Maximum()) ||
	    (result.has_estimated_bytes && result.estimated_bytes == NumericLimits<idx_t>::Maximum())) {
		throw SerializationException("Distributed Paimon scan split contains invalid identity or payload state");
	}
	if ((!result.has_snapshot && result.snapshot_id != 0) ||
	    (!result.has_estimated_cardinality && result.estimated_cardinality != 0) ||
	    (!result.has_estimated_bytes && result.estimated_bytes != 0)) {
		throw SerializationException("Distributed Paimon scan split contains non-canonical absent state");
	}
	return result;
}

static map<string, string> GetPortableOptions(const map<string, string> &options) {
	map<string, string> result;
	for (const auto &key : {paimon::Options::MANIFEST_FORMAT, paimon::Options::FILE_FORMAT,
	                        paimon::Options::SCAN_SNAPSHOT_ID, paimon::Options::SCAN_TIMESTAMP_MILLIS}) {
		auto entry = options.find(key);
		if (entry != options.end()) {
			result.emplace(entry->first, entry->second);
		}
	}
	return result;
}

static bool IsPortableOption(const string &key) {
	return key == paimon::Options::MANIFEST_FORMAT || key == paimon::Options::FILE_FORMAT ||
	       key == paimon::Options::SCAN_SNAPSHOT_ID || key == paimon::Options::SCAN_TIMESTAMP_MILLIS;
}

static void ValidateTransport(const PaimonDistributedBindTransport &transport) {
	if (transport.path.dbname.empty() || transport.path.tablename.empty() || transport.table_data_path.empty() ||
	    transport.table_schema_json.empty() || transport.schema_id < 0 || transport.return_types.empty() ||
	    transport.return_types.size() != transport.return_names.size() || transport.split_set_id.empty()) {
		throw SerializationException("Distributed Paimon scan bind has incomplete portable state");
	}
	for (idx_t index = 0; index < transport.return_types.size(); index++) {
		if (transport.return_types[index].id() == LogicalTypeId::INVALID || transport.return_names[index].empty()) {
			throw SerializationException("Distributed Paimon scan bind contains an invalid output schema");
		}
	}
	unordered_set<string> output_names(transport.return_names.begin(), transport.return_names.end());
	unordered_set<string> part_keys;
	for (const auto &part_key : transport.part_keys) {
		if (part_key.empty() || output_names.find(part_key) == output_names.end() ||
		    !part_keys.insert(part_key).second) {
			throw SerializationException("Distributed Paimon scan bind contains an invalid partition key");
		}
	}
	for (const auto &option : transport.portable_options) {
		if (!IsPortableOption(option.first) || option.second.empty()) {
			throw SerializationException("Distributed Paimon scan bind contains non-portable option '%s'",
			                             option.first);
		}
	}
	if (transport.portable_options.find(paimon::Options::SCAN_SNAPSHOT_ID) != transport.portable_options.end() &&
	    transport.portable_options.find(paimon::Options::SCAN_TIMESTAMP_MILLIS) != transport.portable_options.end()) {
		throw SerializationException("Distributed Paimon scan bind contains conflicting snapshot options");
	}
}

static void SerializeTransport(Serializer &serializer, const PaimonDistributedBindTransport &transport) {
	serializer.WriteProperty(1, "warehouse", transport.path.warehouse);
	serializer.WriteProperty(2, "database", transport.path.dbname);
	serializer.WriteProperty(3, "table", transport.path.tablename);
	serializer.WriteProperty(4, "table_data_path", transport.table_data_path);
	serializer.WriteProperty(5, "table_schema_json", transport.table_schema_json);
	serializer.WriteProperty(6, "schema_id", transport.schema_id);
	serializer.WriteProperty(7, "append_only", transport.append_only);
	serializer.WriteProperty(8, "return_types", transport.return_types);
	serializer.WriteProperty(9, "return_names", transport.return_names);
	serializer.WriteProperty(10, "portable_options", transport.portable_options);
	serializer.WriteProperty(11, "split_set_id", transport.split_set_id);
	serializer.WriteProperty(12, "part_keys", transport.part_keys);
}

static PaimonDistributedBindTransport DeserializeTransport(Deserializer &deserializer) {
	PaimonDistributedBindTransport result;
	result.path.warehouse = deserializer.ReadProperty<string>(1, "warehouse");
	result.path.dbname = deserializer.ReadProperty<string>(2, "database");
	result.path.tablename = deserializer.ReadProperty<string>(3, "table");
	result.table_data_path = deserializer.ReadProperty<string>(4, "table_data_path");
	result.table_schema_json = deserializer.ReadProperty<string>(5, "table_schema_json");
	result.schema_id = deserializer.ReadProperty<int64_t>(6, "schema_id");
	result.append_only = deserializer.ReadProperty<bool>(7, "append_only");
	result.return_types = deserializer.ReadProperty<vector<LogicalType>>(8, "return_types");
	result.return_names = deserializer.ReadProperty<vector<string>>(9, "return_names");
	result.portable_options = deserializer.ReadProperty<map<string, string>>(10, "portable_options");
	result.split_set_id = deserializer.ReadProperty<string>(11, "split_set_id");
	result.part_keys = deserializer.ReadProperty<vector<string>>(12, "part_keys");
	return result;
}

static PaimonDistributedBindTransport GetTransportBase(const PaimonScanBindData &bind) {
	PaimonDistributedBindTransport result;
	result.path = bind.path;
	result.table_data_path = bind.table_data_path;
	result.table_schema_json = bind.table_schema_json;
	result.schema_id = bind.distributed.schema_id;
	result.append_only = bind.distributed.append_only;
	result.return_types = bind.distributed.return_types;
	result.return_names = bind.distributed.return_names;
	result.part_keys = bind.part_keys;
	result.portable_options = bind.distributed.portable_options.empty() ? GetPortableOptions(bind.paimon_options)
	                                                                    : bind.distributed.portable_options;
	result.split_set_id = bind.distributed.split_set_id;
	ValidateTransport(result);
	return result;
}

static const PaimonScanBindData &RequireDistributedScanBindData(const TableFunctionDistributedScanInput &input) {
	if (!input.bind_data) {
		throw InvalidInputException("Distributed Paimon scan requires table-function bind data");
	}
	return input.bind_data->Cast<PaimonScanBindData>();
}

static void ValidateEnvelopeIdentity(const PaimonDistributedSplitEnvelope &envelope,
                                     const PaimonDistributedScanState &state) {
	if (envelope.split_set_id != state.split_set_id || envelope.schema_id != state.schema_id) {
		throw SerializationException("Distributed Paimon scan split does not match its worker bind identity");
	}
}

static void PaimonDistributedScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                                           const TableFunction &) {
	if (!bind_data) {
		throw SerializationException("Cannot serialize empty distributed Paimon scan bind data");
	}
	auto &bind = bind_data->Cast<PaimonScanBindData>();
	PaimonDistributedBindKind bind_kind;
	PaimonDistributedBindTransport transport;
	switch (bind.distributed.phase) {
	case PaimonDistributedScanPhase::COORDINATOR:
		bind_kind = PaimonDistributedBindKind::COORDINATOR;
		transport = GetTransportBase(bind);
		break;
	case PaimonDistributedScanPhase::DISTRIBUTED_COORDINATOR:
		bind_kind = PaimonDistributedBindKind::COORDINATOR;
		transport = GetTransportBase(bind);
		break;
	case PaimonDistributedScanPhase::WORKER_TEMPLATE:
		bind_kind = PaimonDistributedBindKind::WORKER;
		transport = GetTransportBase(bind);
		break;
	case PaimonDistributedScanPhase::WORKER_ASSIGNED:
		throw SerializationException("Assigned distributed Paimon scan splits cannot enter a worker template");
	default:
		throw SerializationException("Distributed Paimon scan bind has an invalid phase");
	}
	ValidateTransport(transport);
	serializer.WriteProperty(1, "bind_kind", static_cast<uint8_t>(bind_kind));
	serializer.WriteObject(2, "scan_bind", [&](Serializer &object) { SerializeTransport(object, transport); });
}

static unique_ptr<FunctionData> PaimonDistributedScanDeserialize(Deserializer &deserializer, TableFunction &) {
	auto &context = deserializer.Get<ClientContext &>();
	auto bind_kind_value = deserializer.ReadProperty<uint8_t>(1, "bind_kind");
	if (bind_kind_value != static_cast<uint8_t>(PaimonDistributedBindKind::COORDINATOR) &&
	    bind_kind_value != static_cast<uint8_t>(PaimonDistributedBindKind::WORKER)) {
		throw SerializationException("Distributed Paimon scan bind has invalid kind %d", bind_kind_value);
	}
	PaimonDistributedBindTransport transport;
	deserializer.ReadObject(2, "scan_bind", [&](Deserializer &object) { transport = DeserializeTransport(object); });
	auto bind_kind = static_cast<PaimonDistributedBindKind>(bind_kind_value);
	ValidateTransport(transport);

	auto result = make_uniq<PaimonScanBindData>();
	result->path = transport.path;
	result->table_data_path = transport.table_data_path;
	result->table_schema_json = transport.table_schema_json;
	result->distributed.schema_id = transport.schema_id;
	result->distributed.append_only = transport.append_only;
	result->distributed.return_types = std::move(transport.return_types);
	result->distributed.return_names = std::move(transport.return_names);
	result->part_keys = std::move(transport.part_keys);
	result->distributed.portable_options = std::move(transport.portable_options);
	result->distributed.split_set_id = std::move(transport.split_set_id);
	result->distributed.phase = bind_kind == PaimonDistributedBindKind::COORDINATOR
	                                ? PaimonDistributedScanPhase::DISTRIBUTED_COORDINATOR
	                                : PaimonDistributedScanPhase::WORKER_TEMPLATE;
	unordered_map<string, Value> local_options;
	result->paimon_options = PaimonCatalog::GetPaimonOptions(context, result->table_data_path, local_options);
	for (const auto &option : result->distributed.portable_options) {
		result->paimon_options[option.first] = option.second;
	}
	return std::move(result);
}

static vector<DistributedScanSplit>
PaimonPlanDistributedScanSplits(const TableFunctionDistributedScanPlanningInput &input) {
	auto &bind = RequireDistributedScanBindData(input);
	if (bind.distributed.phase != PaimonDistributedScanPhase::DISTRIBUTED_COORDINATOR ||
	    bind.distributed.split_set_id.empty()) {
		throw InvalidInputException("Distributed Paimon split planning requires a coordinator bind");
	}
	// DuckDB can serialize a logical table function before complex-filter
	// pushdown. Planning here, instead of in the bind serializer, guarantees
	// that Paimon sees the finalized coordinator predicates and partition filters.
	auto plan = PaimonCreateDistributedScanPlan(bind);
	if (!plan) {
		throw InvalidInputException("Distributed Paimon scan planning returned an empty plan");
	}
	auto snapshot_id = plan->SnapshotId();
	auto has_snapshot = snapshot_id.has_value();
	auto frozen_snapshot_id = snapshot_id.value_or(0);
	auto &splits = plan->Splits();
	if (!has_snapshot && !splits.empty()) {
		throw InvalidInputException("Distributed Paimon scan returned splits without a snapshot identity");
	}

	vector<DistributedScanSplit> result;
	result.reserve(splits.size());
	for (idx_t split_index = 0; split_index < splits.size(); split_index++) {
		auto &paimon_split = splits[split_index];
		if (!paimon_split) {
			throw InvalidInputException("Distributed Paimon scan planned an empty split");
		}
		auto serialized_split = paimon::Split::Serialize(paimon_split, paimon::GetDefaultPool());
		if (!serialized_split.ok()) {
			throw InvalidInputException("Failed to serialize distributed Paimon split: %s",
			                            serialized_split.status().ToString());
		}
		auto estimates = GetSplitEstimates(paimon_split, bind.distributed.append_only);
		PaimonDistributedSplitEnvelope envelope;
		envelope.split_set_id = bind.distributed.split_set_id;
		envelope.split_id = std::to_string(split_index);
		envelope.has_snapshot = has_snapshot;
		envelope.snapshot_id = frozen_snapshot_id;
		envelope.schema_id = bind.distributed.schema_id;
		envelope.paimon_split = std::move(serialized_split).value();
		envelope.has_estimated_cardinality = estimates.has_cardinality;
		envelope.estimated_cardinality = estimates.cardinality;
		envelope.has_estimated_bytes = estimates.has_bytes;
		envelope.estimated_bytes = estimates.bytes;

		DistributedScanSplit split;
		split.split_id = envelope.split_id;
		split.payload = SerializeSplitEnvelope(envelope);
		if (envelope.has_estimated_cardinality) {
			split.estimated_cardinality = optional_idx(envelope.estimated_cardinality);
		}
		if (envelope.has_estimated_bytes) {
			split.estimated_bytes = optional_idx(envelope.estimated_bytes);
		}
		result.push_back(std::move(split));
	}
	return result;
}

static unique_ptr<FunctionData> PaimonCreateDistributedWorkerBind(const TableFunctionDistributedScanInput &input) {
	auto &source = RequireDistributedScanBindData(input);
	if (source.distributed.phase != PaimonDistributedScanPhase::DISTRIBUTED_COORDINATOR ||
	    source.distributed.split_set_id.empty()) {
		throw InvalidInputException("Distributed Paimon worker bind requires a coordinator bind");
	}
	auto result = make_uniq<PaimonScanBindData>();
	result->path = source.path;
	result->table_data_path = source.table_data_path;
	result->table_schema_json = source.table_schema_json;
	result->paimon_options = source.distributed.portable_options;
	result->distributed.phase = PaimonDistributedScanPhase::WORKER_TEMPLATE;
	result->distributed.split_set_id = source.distributed.split_set_id;
	result->distributed.schema_id = source.distributed.schema_id;
	result->distributed.append_only = source.distributed.append_only;
	result->distributed.return_types = source.distributed.return_types;
	result->distributed.return_names = source.distributed.return_names;
	result->part_keys = source.part_keys;
	result->distributed.portable_options = source.distributed.portable_options;
	return std::move(result);
}

static void PaimonApplyDistributedScanSplits(optional_ptr<FunctionData> worker_bind_data,
                                             const vector<DistributedScanSplit> &splits) {
	if (!worker_bind_data) {
		throw InvalidInputException("Distributed Paimon scan requires worker bind data");
	}
	auto &bind = worker_bind_data->Cast<PaimonScanBindData>();
	if (bind.distributed.phase != PaimonDistributedScanPhase::WORKER_TEMPLATE) {
		throw InvalidInputException("Distributed Paimon scan splits require an unassigned worker bind");
	}
	unordered_set<string> split_ids;
	std::vector<std::shared_ptr<paimon::Split>> assigned_splits;
	assigned_splits.reserve(splits.size());
	// The worker template is constructed before Vane invokes plan_splits, so
	// snapshot identity arrives in the assigned split envelopes. Accumulate it
	// locally and commit all assignment state only after every split validates.
	bool has_assignment_snapshot = false;
	int64_t assignment_snapshot_id = 0;
	for (const auto &split : splits) {
		if (!IsCanonicalSplitId(split.split_id) || split.payload.empty() || !split_ids.insert(split.split_id).second) {
			throw InvalidInputException("Invalid or duplicate distributed Paimon scan split '%s'", split.split_id);
		}
		auto envelope = DeserializeSplitEnvelope(split.payload);
		ValidateEnvelopeIdentity(envelope, bind.distributed);
		if (!envelope.has_snapshot) {
			throw InvalidInputException("Distributed Paimon scan split has no snapshot identity");
		}
		if (!has_assignment_snapshot) {
			has_assignment_snapshot = true;
			assignment_snapshot_id = envelope.snapshot_id;
		} else if (envelope.snapshot_id != assignment_snapshot_id) {
			throw InvalidInputException("Distributed Paimon scan splits have inconsistent snapshot identities");
		}
		if (envelope.split_id != split.split_id ||
		    envelope.has_estimated_cardinality != split.estimated_cardinality.IsValid() ||
		    envelope.has_estimated_bytes != split.estimated_bytes.IsValid() ||
		    (envelope.has_estimated_cardinality &&
		     envelope.estimated_cardinality != split.estimated_cardinality.GetIndex()) ||
		    (envelope.has_estimated_bytes && envelope.estimated_bytes != split.estimated_bytes.GetIndex())) {
			throw InvalidInputException("Distributed Paimon scan split metadata does not match its payload");
		}
		auto split_result = paimon::Split::Deserialize(envelope.paimon_split.data(), envelope.paimon_split.size(),
		                                               paimon::GetDefaultPool());
		if (!split_result.ok()) {
			throw InvalidInputException("Failed to deserialize distributed Paimon split: %s",
			                            split_result.status().ToString());
		}
		if (!split_result.value()) {
			throw InvalidInputException("Distributed Paimon split deserialization returned an empty split");
		}
		assigned_splits.push_back(std::move(split_result).value());
	}
	bind.distributed.assigned_splits = std::move(assigned_splits);
	bind.distributed.has_snapshot = has_assignment_snapshot;
	bind.distributed.snapshot_id = assignment_snapshot_id;
	bind.distributed.phase = PaimonDistributedScanPhase::WORKER_ASSIGNED;
}

} // namespace

void InitializePaimonDistributedScanBind(PaimonScanBindData &bind) {
	if (!bind.distributed.split_set_id.empty()) {
		throw InternalException("Distributed Paimon scan bind already has a split-set identity");
	}
	bind.distributed.split_set_id = UUID::ToString(UUID::GenerateRandomUUID());
}

void ConfigurePaimonDistributedScan(TableFunction &function) {
	function.serialize = PaimonDistributedScanSerialize;
	function.deserialize = PaimonDistributedScanDeserialize;
	TableFunctionDistributedScanCallbacks callbacks;
	callbacks.protocol_version = PAIMON_DISTRIBUTED_SCAN_PROTOCOL_VERSION;
	callbacks.split_codec = {PAIMON_DISTRIBUTED_SCAN_SPLIT_CODEC, PAIMON_DISTRIBUTED_SCAN_PROTOCOL_VERSION};
	callbacks.bind_data_mode = TableFunctionDistributedBindDataMode::REQUIRED;
	callbacks.plan_splits = PaimonPlanDistributedScanSplits;
	callbacks.create_worker_bind = PaimonCreateDistributedWorkerBind;
	callbacks.apply_splits = PaimonApplyDistributedScanSplits;
	function.SetDistributedScanCallbacks(std::move(callbacks));
}

} // namespace duckdb
