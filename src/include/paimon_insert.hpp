/*-------------------------------------------------------------------------
 *
 * paimon_insert.hpp
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
 *	  src/include/paimon_insert.hpp
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#ifdef PAIMON_VANE_DISTRIBUTED
#include "duckdb/execution/distributed/extension_write_task_provider.hpp"
#endif
#include "paimon/catalog/identifier.h"

#include <map>
#include <string>

namespace duckdb {

class SchemaCatalogEntry;

class PhysicalPaimonInsert : public PhysicalOperator
#ifdef PAIMON_VANE_DISTRIBUTED
    ,
                             public distributed::ExtensionWriteTaskProvider
#endif
{
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

	PhysicalPaimonInsert(PhysicalPlan &physical_plan, LogicalOperator &op, SchemaCatalogEntry &schema,
	                     unique_ptr<BoundCreateTableInfo> info, paimon::Identifier table_identifier,
	                     map<string, string> paimon_options, vector<string> part_keys, idx_t estimated_cardinality);
#ifdef PAIMON_VANE_DISTRIBUTED
	~PhysicalPaimonInsert() override;
#endif

	SchemaCatalogEntry *schema;
	unique_ptr<BoundCreateTableInfo> info;
	paimon::Identifier table_identifier;
	map<string, string> paimon_options;
	vector<string> part_keys;

#ifdef PAIMON_VANE_DISTRIBUTED
	distributed::DistributedExtensionWritePlan distributed_write_plan;
	string distributed_operation_id;
	string distributed_table_uuid;
	string distributed_table_path;
	string distributed_table_schema_json;
	vector<LogicalType> distributed_input_types;
	vector<string> distributed_input_names;
	string distributed_operation_path;
	map<string, string> distributed_operation_options;
	map<string, string> distributed_portable_options;
	string distributed_null_part_name;
	int64_t distributed_schema_id = -1;
	int64_t distributed_commit_identifier = 0;
	bool distributed_append_only = false;
	bool distributed_has_snapshot = false;
	int64_t distributed_snapshot_id = 0;
	bool distributed_target_initialized = false;
	bool distributed_worker_plan_selected = false;
	mutable bool distributed_operation_open = false;
	mutable bool distributed_recovery_intent_published = false;
	mutable bool distributed_terminal_resolution_started = false;
	mutable bool distributed_finalize_started = false;
#endif

public:
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}
	bool SinkOrderDependent() const override {
		return false;
	}

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSource() const override {
		return true;
	}
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

#ifdef PAIMON_VANE_DISTRIBUTED
	void SetVaneOperationOptions(string table_path, map<string, string> options) {
		distributed_operation_path = std::move(table_path);
		distributed_operation_options = std::move(options);
	}
	void InitializeDistributedWrite(const vector<LogicalType> &input_types);
	optional_ptr<distributed::ExtensionWriteTaskProvider> GetExtensionWriteTaskProvider() override;
	const distributed::DistributedExtensionWritePlan &WritePlan() const override;
	void ValidateDistributedWrite(ClientContext &context) const override;
	idx_t FinalizeDistributedWrite(ClientContext &context,
	                               const vector<DistributedWriteTaskResult> &results) const override;
	void AbortDistributedWrite(ClientContext &context,
	                           const vector<DistributedWriteTaskResult> &selected_results) const override;
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
#endif
};

} // namespace duckdb
