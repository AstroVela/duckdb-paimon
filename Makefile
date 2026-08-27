PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=paimon
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
ENABLE_EXTENSION_AUTOLOADING ?= 1
ENABLE_EXTENSION_AUTOINSTALL ?= 1

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Run Vane-only targets in a recursive Make invocation so their variables never
# enter DuckDB's upstream extension build.
VANE_EXTENSION_MAKEFILE := $(PROJ_DIR)vane-extension-ci-tools/makefiles/vane_extension.Makefile
VANE_EXTENSION_TARGETS := vane_verify_ci_tools vane_validate vane_prepare vane_identity \
	vane_native vane_ci vane_wheel_dependencies vane_wheel
.PHONY: $(VANE_EXTENSION_TARGETS)

$(VANE_EXTENSION_TARGETS):
	@test -f "$(VANE_EXTENSION_MAKEFILE)" || { \
		printf 'initialize vane-extension-ci-tools before running %s\n' "$@" >&2; \
		exit 2; \
	}
	+$(MAKE) --no-print-directory -f "$(VANE_EXTENSION_MAKEFILE)" "$@" \
		VANE_EXTENSION_ROOT="$(abspath $(PROJ_DIR))"
