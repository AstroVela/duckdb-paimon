/*-------------------------------------------------------------------------
 *
 * paimon_scan.hpp
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
 *	  src/include/paimon_scan.hpp
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table_function.hpp"

#include "paimon_functions.hpp"

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace paimon {
class Plan;
class Predicate;
class Split;
} // namespace paimon

namespace duckdb {

#ifdef PAIMON_VANE_DISTRIBUTED
enum class PaimonDistributedScanPhase : uint8_t {
	COORDINATOR = 0,
	DISTRIBUTED_COORDINATOR = 1,
	WORKER_TEMPLATE = 2,
	WORKER_ASSIGNED = 3
};

struct PaimonDistributedScanState {
	PaimonDistributedScanPhase phase = PaimonDistributedScanPhase::COORDINATOR;
	string split_set_id;
	bool has_snapshot = false;
	int64_t snapshot_id = 0;
	int64_t schema_id = -1;
	bool append_only = false;
	vector<LogicalType> return_types;
	vector<string> return_names;
	map<string, string> portable_options;
	std::vector<std::shared_ptr<paimon::Split>> assigned_splits;
};
#endif

struct PaimonScanBindData : public TableFunctionData {
public:
	PaimonTablePath path;
	string table_data_path;

	map<string, string> paimon_options;

	ArrowTableSchema arrow_table;

	std::shared_ptr<paimon::Predicate> predicates = nullptr;

	vector<string> part_keys;
	vector<map<string, string>> part_filters;
	// Debug-only test hook: assert the planned split count before any
	// reader-level or DuckDB-level filtering can affect the result.
	std::optional<idx_t> debug_expected_splits;

	string table_schema_json;

#ifdef PAIMON_VANE_DISTRIBUTED
	PaimonDistributedScanState distributed;
#endif
};

#ifdef PAIMON_VANE_DISTRIBUTED
std::shared_ptr<paimon::Plan> PaimonCreateDistributedScanPlan(const PaimonScanBindData &bind);
void InitializePaimonDistributedScanBind(PaimonScanBindData &bind);
void ConfigurePaimonDistributedScan(TableFunction &function);
#endif

} // namespace duckdb
