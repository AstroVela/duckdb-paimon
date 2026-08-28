cmake_minimum_required(VERSION 3.17)

# Set extension name here
set(TARGET_NAME paimon)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

option(PAIMON_VANE_DISTRIBUTED
       "Build the explicit Vane distributed Paimon interfaces" OFF)
if(PAIMON_VANE_DISTRIBUTED)
  if(NOT EXISTS "${DUCKDB_SOURCE_PATH}/src/include/duckdb/function/distributed_table_function.hpp")
    message(FATAL_ERROR "PAIMON_VANE_DISTRIBUTED requires the Vane DuckDB source tree")
  endif()
  add_definitions(-DPAIMON_VANE_DISTRIBUTED)
endif()

# Import paimon-cpp as a subproject
include(ExternalProject)

# JindoSDK: the installed filename differs by platform, but we always bundle
# using the short name that matches the dylib's install_name.
file(STRINGS third_party/paimon-cpp/third_party/versions.txt
     _jindosdk_ver REGEX "^PAIMON_JINDOSDK_C_BUILD_VERSION=")
string(REGEX REPLACE ".*=" "" _jindosdk_ver "${_jindosdk_ver}")
string(REGEX REPLACE "([0-9]+)\\..*" "\\1" _jindosdk_major "${_jindosdk_ver}")
if(APPLE)
  set(JINDOSDK_C_INSTALLED_NAME "libjindosdk_c.${_jindosdk_ver}.dylib")
  set(JINDOSDK_C_RUNTIME_NAME "libjindosdk_c.${_jindosdk_major}.dylib")
else()
  set(JINDOSDK_C_INSTALLED_NAME "libjindosdk_c.so")
  set(JINDOSDK_C_RUNTIME_NAME "libjindosdk_c.so.${_jindosdk_major}")
endif()

set(PAIMON_CPP_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/paimon-cpp)
set(PAIMON_CPP_PREFIX ${CMAKE_CURRENT_BINARY_DIR}/paimon-cpp-install)
set(PAIMON_CPP_INCLUDE ${PAIMON_CPP_PREFIX}/include)
set(PAIMON_CPP_LIB ${PAIMON_CPP_PREFIX}/lib64)
file(MAKE_DIRECTORY ${PAIMON_CPP_INCLUDE})

# paimon-cpp's bundled AWS projects install package metadata below lib64 while
# the downstream projects discover packages below lib. Keep this build-tree
# alias isolated to Vane; the native DuckDB build retains its existing layout.
if(PAIMON_VANE_DISTRIBUTED AND UNIX AND NOT APPLE)
  set(PAIMON_VANE_AWS_PREFIX
      "${CMAKE_CURRENT_BINARY_DIR}/paimon_cpp_ep-prefix/src/paimon_cpp_ep-build/aws-auth_ep-install")
  file(MAKE_DIRECTORY "${PAIMON_VANE_AWS_PREFIX}/lib64")
  if(NOT EXISTS "${PAIMON_VANE_AWS_PREFIX}/lib" AND
     NOT IS_SYMLINK "${PAIMON_VANE_AWS_PREFIX}/lib")
    file(CREATE_LINK lib64 "${PAIMON_VANE_AWS_PREFIX}/lib" SYMBOLIC)
  endif()
endif()

set(PAIMON_CPP_PATCHES
  ${CMAKE_CURRENT_SOURCE_DIR}/patches/paimon-cpp-install-arrow-headers.patch)

# Build patch command: apply each patch idempotently
set(PAIMON_CPP_PATCH_SCRIPT "")
foreach(patch IN LISTS PAIMON_CPP_PATCHES)
  string(APPEND PAIMON_CPP_PATCH_SCRIPT
    "git -C ${PAIMON_CPP_SOURCE_DIR} apply --reverse --check ${patch} 2>/dev/null || git -C ${PAIMON_CPP_SOURCE_DIR} apply ${patch} && ")
endforeach()
string(APPEND PAIMON_CPP_PATCH_SCRIPT "true")

set(PAIMON_CPP_TOOLCHAIN_ARGS)
set(PAIMON_CPP_VCPKG_LIBS)
if(CMAKE_TOOLCHAIN_FILE AND DEFINED VCPKG_TARGET_TRIPLET AND DEFINED VCPKG_INSTALLED_DIR)
  list(APPEND PAIMON_CPP_TOOLCHAIN_ARGS
    -DCMAKE_TOOLCHAIN_FILE=${CMAKE_TOOLCHAIN_FILE}
    -DVCPKG_TARGET_TRIPLET=${VCPKG_TARGET_TRIPLET}
    -DVCPKG_INSTALLED_DIR=${VCPKG_INSTALLED_DIR}
    -DVCPKG_MANIFEST_MODE=OFF)
  if(UNIX AND NOT APPLE)
    find_package(CURL REQUIRED)
    find_package(OpenSSL REQUIRED)
    list(APPEND PAIMON_CPP_VCPKG_LIBS CURL::libcurl OpenSSL::SSL OpenSSL::Crypto)
  endif()
endif()

ExternalProject_Add(paimon_cpp_ep
  SOURCE_DIR ${PAIMON_CPP_SOURCE_DIR}
  PATCH_COMMAND
    sh -c "${PAIMON_CPP_PATCH_SCRIPT}"
  CMAKE_ARGS
    -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
    -DCMAKE_INSTALL_PREFIX=${PAIMON_CPP_PREFIX}
    -DCMAKE_INSTALL_LIBDIR=lib64
    -DLIBUNWIND_LIBRARY=FALSE
    -DPAIMON_DEPENDENCY_SOURCE=BUNDLED
    -DPAIMON_BUILD_TESTS=OFF
    -DPAIMON_BUILD_SHARED=ON
    -DPAIMON_BUILD_STATIC=OFF
    -DPAIMON_ENABLE_JINDO=ON
    -DPAIMON_ENABLE_S3=ON
    -DPAIMON_ENABLE_REST=ON
    -DPAIMON_ENABLE_ORC=ON
    -DPAIMON_ENABLE_AVRO=ON
    -DPAIMON_ENABLE_LUMINA=OFF
    -DPAIMON_ENABLE_LUCENE=OFF
    ${PAIMON_CPP_TOOLCHAIN_ARGS}
  BUILD_BYPRODUCTS
    ${PAIMON_CPP_LIB}/libpaimon${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_file_index${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_global_index${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_local_file_system${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_jindo_file_system${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_s3_file_system${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/${JINDOSDK_C_INSTALLED_NAME}
    ${PAIMON_CPP_LIB}/libpaimon_parquet_file_format${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_orc_file_format${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_avro_file_format${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_blob_file_format${CMAKE_SHARED_LIBRARY_SUFFIX}
)

# On macOS, paimon-cpp installs the JindoSDK dylib with a versioned filename
# (e.g. libjindosdk_c.6.10.2.dylib) but its install_name uses the short
# version (libjindosdk_c.6.dylib). Add a post-install step to provide the
# short-named copy that runtime lookup expects.
if(APPLE)
  ExternalProject_Add_Step(paimon_cpp_ep jindosdk_short_name
    DEPENDEES install
    BYPRODUCTS ${PAIMON_CPP_LIB}/${JINDOSDK_C_RUNTIME_NAME}
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
      ${PAIMON_CPP_LIB}/${JINDOSDK_C_INSTALLED_NAME}
      ${PAIMON_CPP_LIB}/${JINDOSDK_C_RUNTIME_NAME})
endif()

# Extension sources
set(EXTENSION_SOURCES
  src/paimon_extension.cpp
  src/paimon_functions.cpp
  src/paimon_storage/paimon_scan.cpp
  src/paimon_storage/paimon_snapshots.cpp
  src/paimon_storage/paimon_catalog.cpp
  src/paimon_storage/paimon_insert.cpp
  src/paimon_storage/paimon_schema_entry.cpp
  src/paimon_storage/paimon_schema_set.cpp
  src/paimon_storage/paimon_table_entry.cpp
  src/paimon_storage/paimon_table_set.cpp
  src/paimon_storage/paimon_transaction_manager.cpp
  src/paimon_storage/paimon_type_utils.cpp)

if(PAIMON_VANE_DISTRIBUTED)
  list(APPEND EXTENSION_SOURCES
       src/paimon_storage/paimon_distributed_scan.cpp)
endif()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Include paimon-cpp and arrow headers
target_include_directories(${EXTENSION_NAME} SYSTEM PRIVATE ${PAIMON_CPP_INCLUDE})
target_include_directories(${TARGET_NAME}_loadable_extension SYSTEM PRIVATE ${PAIMON_CPP_INCLUDE})

# Build paimon-cpp before building the extension
add_dependencies(${EXTENSION_NAME} paimon_cpp_ep)
add_dependencies(${TARGET_NAME}_loadable_extension paimon_cpp_ep)

# Link with paimon-cpp
set(PAIMON_CPP_LIBS
  ${PAIMON_CPP_LIB}/libpaimon${CMAKE_SHARED_LIBRARY_SUFFIX}
  $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--no-as-needed>
  ${PAIMON_CPP_LIB}/libpaimon_file_index${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_global_index${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_local_file_system${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_jindo_file_system${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_s3_file_system${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/${JINDOSDK_C_INSTALLED_NAME}
  ${PAIMON_CPP_LIB}/libpaimon_parquet_file_format${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_orc_file_format${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_avro_file_format${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_blob_file_format${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_VCPKG_LIBS}
  $<$<NOT:$<PLATFORM_ID:Darwin>>:-Wl,--as-needed>
)
target_link_libraries(${EXTENSION_NAME} ${PAIMON_CPP_LIBS})
target_link_libraries(${TARGET_NAME}_loadable_extension ${PAIMON_CPP_LIBS})

if(APPLE)
  # Propagate rpath so unittest/shell can find paimon-cpp shared libraries.
  target_link_options(${EXTENSION_NAME} INTERFACE "LINKER:-rpath,${PAIMON_CPP_LIB}")
  set(PAIMON_RPATH "@loader_path")
else()
  set(PAIMON_RPATH "\$ORIGIN")
endif()

# Bundle paimon-cpp runtime dependencies next to paimon.duckdb_extension.
set(PAIMON_CPP_RUNTIME_BUNDLE_LIBS
  ${PAIMON_CPP_LIB}/libpaimon${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_file_index${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_global_index${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_local_file_system${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_jindo_file_system${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_s3_file_system${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/${JINDOSDK_C_RUNTIME_NAME}
  ${PAIMON_CPP_LIB}/libpaimon_parquet_file_format${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_orc_file_format${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_avro_file_format${CMAKE_SHARED_LIBRARY_SUFFIX}
  ${PAIMON_CPP_LIB}/libpaimon_blob_file_format${CMAKE_SHARED_LIBRARY_SUFFIX}
)

# A Vane wheel embeds DuckDB and its extensions in vane._native. Install the
# Paimon runtime DSOs beside that module and resolve them relative to the wheel
# instead of retaining a build-tree RUNPATH. Native DuckDB packaging continues
# to use the existing loadable-extension bundle below.
if(PAIMON_VANE_DISTRIBUTED AND DEFINED SKBUILD_PLATLIB_DIR AND UNIX AND
   NOT APPLE)
  set(PAIMON_VANE_WHEEL_RUNTIME_LIBS
    ${PAIMON_CPP_LIB}/libpaimon${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_file_index${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_global_index${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_local_file_system${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_jindo_file_system${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_s3_file_system${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_parquet_file_format${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_orc_file_format${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_avro_file_format${CMAKE_SHARED_LIBRARY_SUFFIX}
    ${PAIMON_CPP_LIB}/libpaimon_blob_file_format${CMAKE_SHARED_LIBRARY_SUFFIX})

  # Vane adds DuckDB with EXCLUDE_FROM_ALL, so install rules declared in this
  # extension directory are not reached by scikit-build-core. Defer the wheel
  # rules into Vane's top-level directory, after its _native target exists.
  set(PAIMON_VANE_WHEEL_RUNTIME_LIBS
      "${PAIMON_VANE_WHEEL_RUNTIME_LIBS}"
      CACHE INTERNAL "Paimon runtime libraries bundled in a Vane wheel" FORCE)
  set(PAIMON_VANE_WHEEL_JINDO_SOURCE
      "${PAIMON_CPP_LIB}/libjindosdk_c.so.${_jindosdk_ver}"
      CACHE INTERNAL "JindoSDK library bundled in a Vane wheel" FORCE)
  set(PAIMON_VANE_WHEEL_JINDO_NAME
      "${JINDOSDK_C_RUNTIME_NAME}"
      CACHE INTERNAL "JindoSDK runtime name used in a Vane wheel" FORCE)
  set(PAIMON_VANE_WHEEL_LICENSE_ROOT
      "${PAIMON_CPP_SOURCE_DIR}"
      CACHE INTERNAL "Paimon license source bundled in a Vane wheel" FORCE)

  cmake_language(DEFER DIRECTORY "${CMAKE_SOURCE_DIR}" CALL set_property
                 TARGET _native PROPERTY INSTALL_RPATH "\$ORIGIN")
  cmake_language(DEFER DIRECTORY "${CMAKE_SOURCE_DIR}" CALL install FILES
                 ${PAIMON_VANE_WHEEL_RUNTIME_LIBS} DESTINATION vane)
  cmake_language(DEFER DIRECTORY "${CMAKE_SOURCE_DIR}" CALL install FILES
                 ${PAIMON_VANE_WHEEL_JINDO_SOURCE} DESTINATION vane RENAME
                 ${PAIMON_VANE_WHEEL_JINDO_NAME})
  cmake_language(DEFER DIRECTORY "${CMAKE_SOURCE_DIR}" CALL install FILES
                 ${PAIMON_VANE_WHEEL_LICENSE_ROOT}/LICENSE
                 ${PAIMON_VANE_WHEEL_LICENSE_ROOT}/NOTICE DESTINATION
                 vane/licenses/paimon-cpp)
  cmake_language(DEFER DIRECTORY "${CMAKE_SOURCE_DIR}" CALL install DIRECTORY
                 ${PAIMON_VANE_WHEEL_LICENSE_ROOT}/licenses/ DESTINATION
                 vane/licenses/paimon-cpp/third-party)
  cmake_language(DEFER DIRECTORY "${CMAKE_SOURCE_DIR}" CALL install FILES
                 ${PAIMON_VANE_WHEEL_LICENSE_ROOT}/third_party/jindosdk-nextarch/LICENSE.txt
                 ${PAIMON_VANE_WHEEL_LICENSE_ROOT}/third_party/jindosdk-nextarch/NOTICE.txt
                 DESTINATION vane/licenses/paimon-cpp/jindosdk-nextarch)
endif()

foreach(lib IN LISTS PAIMON_CPP_RUNTIME_BUNDLE_LIBS)
  add_custom_command(
    TARGET ${TARGET_NAME}_loadable_extension
    POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_if_different
      ${lib}
      $<TARGET_FILE_DIR:${TARGET_NAME}_loadable_extension>
  )
endforeach()
set_target_properties(${TARGET_NAME}_loadable_extension PROPERTIES
  BUILD_RPATH "${PAIMON_RPATH}"
  INSTALL_RPATH "${PAIMON_RPATH}"
  BUILD_WITH_INSTALL_RPATH ON
  INSTALL_RPATH_USE_LINK_PATH OFF
)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
