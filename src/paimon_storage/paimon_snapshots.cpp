/*-------------------------------------------------------------------------
 *
 * paimon_snapshots.cpp
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
 *      src/paimon_storage/paimon_snapshots.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "duckdb.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"

#ifdef PAIMON_VANE_DISTRIBUTED
#include "duckdb/function/distributed_table_function.hpp"
#endif

#include "paimon_catalog.hpp"
#include "paimon_functions.hpp"

#include "paimon/catalog/identifier.h"
#include "paimon/snapshot/snapshot_info.h"

namespace duckdb {

static vector<LogicalType> SnapshotTypes() {
	return {LogicalType::BIGINT,    LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
	        LogicalType::TIMESTAMP, LogicalType::BIGINT, LogicalType::BIGINT,  LogicalType::BIGINT};
}

struct PaimonSnapshotsBindData : public TableFunctionData {
	// Resolve the catalog once. Bound plans and retries retain these completed rows.
	vector<vector<Value>> rows;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<PaimonSnapshotsBindData>();
		result->rows = rows;
		return std::move(result);
	}

	bool Equals(const FunctionData &other) const override {
		return rows == other.Cast<PaimonSnapshotsBindData>().rows;
	}

	static void Serialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data, const TableFunction &) {
		serializer.WriteProperty(100, "rows", bind_data->Cast<PaimonSnapshotsBindData>().rows);
	}

	static unique_ptr<FunctionData> Deserialize(Deserializer &deserializer, TableFunction &) {
		auto result = make_uniq<PaimonSnapshotsBindData>();
		result->rows = deserializer.ReadProperty<vector<vector<Value>>>(100, "rows");
		auto types = SnapshotTypes();
		for (auto &row : result->rows) {
			if (row.size() != types.size()) {
				throw SerializationException("Paimon snapshot row has an invalid column count");
			}
			for (idx_t col = 0; col < types.size(); col++) {
				if (row[col].type() != types[col]) {
					throw SerializationException("Paimon snapshot row has an invalid column type");
				}
			}
		}
		return std::move(result);
	}
};

struct PaimonSnapshotsGlobalState : public GlobalTableFunctionState {
	idx_t current_row = 0;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> PaimonSnapshotsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<PaimonSnapshotsBindData>();

	auto path = PaimonTablePath::Parse(input.inputs);
	auto options = unordered_map<string, Value>(input.named_parameters.begin(), input.named_parameters.end());
	auto catalog = PaimonCatalog::CreatePaimonCatalog(context, path.warehouse, options);
	auto snapshots = catalog->ListSnapshots(paimon::Identifier(path.dbname, path.tablename));
	if (!snapshots.ok()) {
		throw IOException(snapshots.status().ToString());
	}
	for (auto &snapshot : snapshots.value()) {
		timestamp_t timestamp;
		timestamp.value = snapshot.time_millis * 1000;
		bind_data->rows.push_back(
		    {Value::BIGINT(snapshot.snapshot_id), Value::BIGINT(snapshot.schema_id), Value(snapshot.commit_user),
		     Value(paimon::SnapshotInfo::CommitKindToString(snapshot.commit_kind)), Value::TIMESTAMP(timestamp),
		     snapshot.total_record_count ? Value::BIGINT(snapshot.total_record_count.value())
		                                 : Value(LogicalType::BIGINT),
		     snapshot.delta_record_count ? Value::BIGINT(snapshot.delta_record_count.value())
		                                 : Value(LogicalType::BIGINT),
		     snapshot.watermark ? Value::BIGINT(snapshot.watermark.value()) : Value(LogicalType::BIGINT)});
	}
	names = {"snapshot_id", "schema_id",          "commit_user",        "commit_kind",
	         "commit_time", "total_record_count", "delta_record_count", "watermark"};
	return_types = SnapshotTypes();

	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState> PaimonSnapshotsInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	return make_uniq<PaimonSnapshotsGlobalState>();
}

static void PaimonSnapshotsExecute(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<PaimonSnapshotsGlobalState>();
	auto &rows = input.bind_data->Cast<PaimonSnapshotsBindData>().rows;
	idx_t count = 0;
	while (state.current_row < rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = rows[state.current_row++];
		for (idx_t col = 0; col < row.size(); col++) {
			output.SetValue(col, count, row[col]);
		}
		count++;
	}
	output.SetCardinality(count);
}

static void ConfigureSnapshotsFunction(TableFunction &function) {
	function.named_parameters["manifest_format"] = LogicalType::VARCHAR; // deprecated: inferred from table schema
	function.serialize = PaimonSnapshotsBindData::Serialize;
	function.deserialize = PaimonSnapshotsBindData::Deserialize;
#ifdef PAIMON_VANE_DISTRIBUTED
	function.SetDistributedScanCallbacks(MakeDistributedSingletonSourceCallbacks());
#endif
}

TableFunctionSet PaimonFunctions::GetPaimonSnapshotsFunction() {
	TableFunctionSet function_set("paimon_snapshots");

	auto fun = TableFunction({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, PaimonSnapshotsExecute,
	                         PaimonSnapshotsBind, PaimonSnapshotsInitGlobal);
	ConfigureSnapshotsFunction(fun);
	function_set.AddFunction(fun);

	auto fun_fullpath =
	    TableFunction({LogicalType::VARCHAR}, PaimonSnapshotsExecute, PaimonSnapshotsBind, PaimonSnapshotsInitGlobal);
	ConfigureSnapshotsFunction(fun_fullpath);
	function_set.AddFunction(fun_fullpath);

	return function_set;
}

} // namespace duckdb
