// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <vector>

#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "olap/field.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_variant.h"
#include "vec/common/variant_util.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_variant.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
static doris::vectorized::Field construct_variant_map(
        const std::vector<std::pair<std::string, doris::vectorized::Field>>& key_and_values) {
    doris::vectorized::Field res = Field::create_field<TYPE_VARIANT>(VariantMap {});
    auto& object = res.get<TYPE_VARIANT>();
    for (const auto& [k, v] : key_and_values) {
        PathInData path(k);
        object.try_emplace(path, v);
    }
    return res;
}

static auto construct_basic_varint_column() {
    // 1. create an empty variant column
    auto variant = ColumnVariant::create(5);

    std::vector<std::pair<std::string, doris::vectorized::Field>> data;

    // 2. subcolumn path
    data.emplace_back("v.a", Field::create_field<TYPE_INT>(20));
    data.emplace_back("v.b", Field::create_field<TYPE_STRING>("20"));
    data.emplace_back("v.c", Field::create_field<TYPE_INT>(20));
    data.emplace_back("v.f", Field::create_field<TYPE_INT>(20));
    data.emplace_back("v.e", Field::create_field<TYPE_STRING>("50"));
    for (int i = 0; i < 5; ++i) {
        auto field = construct_variant_map(data);
        variant->try_insert(field);
    }

    return variant;
}

TEST(FunctionVariantCast, CastToVariant) {
    // Test casting from basic types to variant
    {
        // Test Int32 to variant
        auto int32_type = std::make_shared<DataTypeInt32>();
        auto variant_type = std::make_shared<DataTypeVariant>();
        auto int32_col = ColumnInt32::create();
        int32_col->insert(Field::create_field<TYPE_INT>(42));
        int32_col->insert(Field::create_field<TYPE_INT>(100));
        int32_col->insert(Field::create_field<TYPE_INT>(-1));

        ColumnsWithTypeAndName arguments {{int32_col->get_ptr(), int32_type, "int32_col"},
                                          {nullptr, variant_type, "variant_type"}};

        auto function =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, variant_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, variant_type, "result"});

        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 3).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        const auto* variant_col = assert_cast<const ColumnVariant*>(result_col.get());
        ASSERT_EQ(variant_col->size(), 3);
    }

    // Test casting from string to variant
    {
        auto string_type = std::make_shared<DataTypeString>();
        auto variant_type = std::make_shared<DataTypeVariant>();
        auto string_col = ColumnString::create();
        string_col->insert_data("hello", 5);
        string_col->insert_data("world", 5);

        ColumnsWithTypeAndName arguments {{string_col->get_ptr(), string_type, "string_col"},
                                          {nullptr, variant_type, "variant_type"}};

        auto function = SimpleFunctionFactory::instance().get_function("CAST", arguments,
                                                                       make_nullable(variant_type));
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, variant_type, "result"});

        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 2).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        const auto* variant_col =
                assert_cast<const ColumnVariant*>(remove_nullable(result_col).get());
        ASSERT_EQ(variant_col->size(), 2);
    }

    // Test casting from array to variant
    {
        auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
        auto variant_type = std::make_shared<DataTypeVariant>();
        auto array_col =
                ColumnArray::create(ColumnInt32::create(), ColumnArray::ColumnOffsets::create());
        auto& data = assert_cast<ColumnInt32&>(array_col->get_data());
        auto& offsets = array_col->get_offsets();

        data.insert(Field::create_field<TYPE_INT>(1));
        data.insert(Field::create_field<TYPE_INT>(2));
        data.insert(Field::create_field<TYPE_INT>(3));
        offsets.push_back(3);

        ColumnsWithTypeAndName arguments {{array_col->get_ptr(), array_type, "array_col"},
                                          {nullptr, variant_type, "variant_type"}};

        auto function =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, variant_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, variant_type, "result"});

        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 1).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        const auto* variant_col =
                assert_cast<const ColumnVariant*>(remove_nullable(result_col).get());
        ASSERT_EQ(variant_col->size(), 1);
    }
}

TEST(FunctionVariantCast, CastFromVariant) {
    // Test casting from variant to basic types
    {
        auto variant_type = std::make_shared<DataTypeVariant>();
        auto int32_type = std::make_shared<DataTypeInt32>();
        auto variant_col = ColumnVariant::create(0);

        // Create a variant column with integer values
        variant_col->create_root(int32_type, ColumnInt32::create());
        MutableColumnPtr data = variant_col->get_root();
        data->insert(Field::create_field<TYPE_INT>(42));
        data->insert(Field::create_field<TYPE_INT>(100));
        data->insert(Field::create_field<TYPE_INT>(-1));

        ColumnsWithTypeAndName arguments {{variant_col->get_ptr(), variant_type, "variant_col"},
                                          {nullptr, int32_type, "int32_type"}};

        auto function =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, int32_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, int32_type, "result"});

        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 3).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        // always nullable
        const auto* int32_result =
                assert_cast<const ColumnInt32*>(remove_nullable(result_col).get());
        ASSERT_EQ(int32_result->size(), 3);
        ASSERT_EQ(int32_result->get_element(0), 42);
        ASSERT_EQ(int32_result->get_element(1), 100);
        ASSERT_EQ(int32_result->get_element(2), -1);
    }

    // Test casting from variant to string
    {
        auto variant_type = std::make_shared<DataTypeVariant>();
        auto string_type = std::make_shared<DataTypeString>();
        auto variant_col = ColumnVariant::create(0);

        // Create a variant column with string values
        variant_col->create_root(string_type, ColumnString::create());
        MutableColumnPtr data = variant_col->get_root();
        data->insert_data("hello", 5);
        data->insert_data("world", 5);

        ColumnsWithTypeAndName arguments {{variant_col->get_ptr(), variant_type, "variant_col"},
                                          {nullptr, string_type, "string_type"}};

        auto function =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, string_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, string_type, "result"});

        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 2).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        const auto* string_result =
                assert_cast<const ColumnString*>(remove_nullable(result_col).get());
        ASSERT_EQ(string_result->size(), 2);
        ASSERT_EQ(string_result->get_data_at(0).to_string(), "hello");
        ASSERT_EQ(string_result->get_data_at(1).to_string(), "world");
    }

    // Test casting from variant to array
    {
        auto variant_type = std::make_shared<DataTypeVariant>();
        auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());
        auto variant_col = ColumnVariant::create(0);

        // Create a variant column with array values
        variant_col->create_root(
                array_type,
                ColumnArray::create(ColumnInt32::create(), ColumnArray::ColumnOffsets::create()));
        MutableColumnPtr data = variant_col->get_root();

        Field a = Field::create_field<TYPE_ARRAY>(Array {Field::create_field<TYPE_INT>(1),
                                                         Field::create_field<TYPE_INT>(2),
                                                         Field::create_field<TYPE_INT>(3)});

        data->insert(a);

        ColumnsWithTypeAndName arguments {{variant_col->get_ptr(), variant_type, "variant_col"},
                                          {nullptr, array_type, "array_type"}};

        auto function =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, array_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, array_type, "result"});

        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 1).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        const auto* array_result =
                assert_cast<const ColumnArray*>(remove_nullable(result_col).get());
        ASSERT_EQ(array_result->size(), 1);
        const auto& result_data = assert_cast<const ColumnInt32&>(array_result->get_data());
        ASSERT_EQ(result_data.size(), 3);
        ASSERT_EQ(result_data.get_element(0), 1);
        ASSERT_EQ(result_data.get_element(1), 2);
        ASSERT_EQ(result_data.get_element(2), 3);
    }
}

TEST(FunctionVariantCast, CastVariantWithNull) {
    auto variant_type = std::make_shared<DataTypeVariant>();
    auto int32_type = std::make_shared<DataTypeInt32>();
    auto nullable_int32_type = std::make_shared<DataTypeNullable>(int32_type);

    // Create a variant column with nullable integer values
    auto variant_col = ColumnVariant::create(0);
    variant_col->create_root(nullable_int32_type,
                             ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create()));
    MutableColumnPtr data = variant_col->get_root();

    data->insert(Field::create_field<TYPE_INT>(42));
    data->insert(Field::create_field<TYPE_NULL>(Null()));
    data->insert(Field::create_field<TYPE_INT>(100));

    ColumnsWithTypeAndName arguments {{variant_col->get_ptr(), variant_type, "variant_col"},
                                      {nullptr, nullable_int32_type, "nullable_int32_type"}};

    variant_col->finalize();
    auto function =
            SimpleFunctionFactory::instance().get_function("CAST", arguments, nullable_int32_type);
    ASSERT_NE(function, nullptr);

    Block block {arguments};
    size_t result_column = block.columns();
    block.insert({nullptr, nullable_int32_type, "result"});

    RuntimeState state;
    auto ctx = FunctionContext::create_context(&state, {}, {});
    ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 3).ok());

    auto result_col = block.get_by_position(result_column).column;
    ASSERT_NE(result_col.get(), nullptr);
    const auto* nullable_result = assert_cast<const ColumnNullable*>(result_col.get());
    ASSERT_EQ(nullable_result->size(), 3);

    const auto& result_data = assert_cast<const ColumnInt32&>(nullable_result->get_nested_column());
    const auto& result_null_map = nullable_result->get_null_map_data();

    ASSERT_EQ(result_data.get_element(0), 42);
    ASSERT_EQ(result_null_map[0], 0);
    ASSERT_EQ(result_null_map[1], 1);
    ASSERT_EQ(result_data.get_element(2), 100);
}

TEST(FunctionVariantCast, CastFromVariantWithEmptyRoot) {
    // Test case 1: variant.empty() branch
    {
        auto variant_type = std::make_shared<DataTypeVariant>();
        auto int32_type = std::make_shared<DataTypeInt32>();
        MutableColumnPtr root = ColumnInt32::create();
        root->insert(Field::create_field<TYPE_INT>(42));
        vectorized::ColumnVariant::Subcolumns dynamic_subcolumns;
        dynamic_subcolumns.add(
                vectorized::PathInData(ColumnVariant::COLUMN_NAME_DUMMY),
                vectorized::ColumnVariant::Subcolumn {root->get_ptr(), int32_type, true, true});
        auto variant_col = ColumnVariant::create(0, std::move(dynamic_subcolumns));

        variant_col->finalize();
        ColumnsWithTypeAndName arguments {{variant_col->get_ptr(), variant_type, "variant_col"},
                                          {nullptr, int32_type, "int32_type"}};

        auto function =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, int32_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, int32_type, "result"});

        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 1).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        // always nullable
        const auto* int32_result =
                assert_cast<const ColumnInt32*>(remove_nullable(result_col).get());
        ASSERT_EQ(int32_result->size(), 1);
        // because of variant.empty() we insert_default with data_type_to
        ASSERT_EQ(int32_result->get_element(0), 0);
    }

    // Test case 2: !data_type_to->is_nullable() && !WhichDataType(data_type_to).is_string() branch
    {
        // object has sparse column
        auto int32_type = std::make_shared<DataTypeInt32>();
        auto variant_col = construct_basic_varint_column();
        auto variant_type = std::make_shared<DataTypeVariant>();

        ColumnsWithTypeAndName arguments {{variant_col->get_ptr(), variant_type, "variant_col"},
                                          {nullptr, int32_type, "int32_type"}};

        variant_col->finalize();
        auto function =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, int32_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, int32_type, "result"});
        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 1).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        const auto* nullable_result = assert_cast<const ColumnNullable*>(result_col.get());
        ASSERT_EQ(nullable_result->size(), 1);
        ASSERT_TRUE(nullable_result->is_null_at(0));
    }

    // Test case 3: WhichDataType(data_type_to).is_string() branch
    {
        // variant has sparse column
        auto int32_type = std::make_shared<DataTypeInt32>();
        auto variant_col = construct_basic_varint_column();

        auto string_type = std::make_shared<DataTypeString>();
        auto variant_type = std::make_shared<DataTypeVariant>();

        ColumnsWithTypeAndName arguments {{variant_col->get_ptr(), variant_type, "variant_col"},
                                          {nullptr, string_type, "string_type"}};

        variant_col->finalize();
        auto function =
                SimpleFunctionFactory::instance().get_function("CAST", arguments, string_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, string_type, "result"});
        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 1).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        const auto* string_result = assert_cast<const ColumnString*>(result_col.get());
        // just call ConvertImplGenericToString which will insert all source column data to ColumnString
        ASSERT_EQ(string_result->size(), variant_col->size());
        ASSERT_EQ(string_result->get_data_at(0).to_string(),
                  "{\"v\":{\"a\":20,\"b\":\"20\",\"c\":20,\"e\":\"50\",\"f\":20}}");
    }

    // Test case 4: else branch (nullable type)
    {
        auto variant_col = construct_basic_varint_column();
        variant_col->finalize();
        auto nullable_variant_col = make_nullable(variant_col->get_ptr());

        auto nullable_string_type = make_nullable(std::make_shared<DataTypeString>());
        auto variant_type = std::make_shared<DataTypeVariant>();
        auto nullable_variant_type = make_nullable(variant_type);

        ColumnsWithTypeAndName arguments {
                {nullable_variant_col->get_ptr(), nullable_variant_type, "variant_col"},
                {nullptr, nullable_string_type, "nullable_string_type"}};

        auto function = SimpleFunctionFactory::instance().get_function("CAST", arguments,
                                                                       nullable_string_type);
        ASSERT_NE(function, nullptr);

        Block block {arguments};
        size_t result_column = block.columns();
        block.insert({nullptr, nullable_string_type, "result"});
        RuntimeState state;
        auto ctx = FunctionContext::create_context(&state, {}, {});
        ASSERT_TRUE(function->execute(ctx.get(), block, {0}, result_column, 1).ok());

        auto result_col = block.get_by_position(result_column).column;
        ASSERT_NE(result_col.get(), nullptr);
        const auto* nullable_result = assert_cast<const ColumnNullable*>(result_col.get());
        ASSERT_EQ(nullable_result->size(), 1);
        ASSERT_TRUE(nullable_result->is_null_at(1));
    }
}

// Build a variant column with many subcolumns to stress cast paths.
static MutableColumnPtr build_dense_variant_column(size_t num_rows, size_t num_subcols,
                                                   int32_t max_subcolumns_count) {
    auto variant_col = ColumnVariant::create(max_subcolumns_count);
    // Spread subcolumns across nested paths to mimic complex JSON/Variant documents.
    size_t outer = std::max<size_t>(1, static_cast<size_t>(std::sqrt(num_subcols)));
    size_t inner = (num_subcols + outer - 1) / outer;
    for (size_t row = 0; row < num_rows; ++row) {
        VariantMap object;
        for (size_t c = 0; c < num_subcols; ++c) {
            size_t outer_id = c / inner;
            size_t inner_id = c % inner;
            std::string path =
                    "obj" + std::to_string(outer_id) + ".field" + std::to_string(inner_id);
            // keep values unique but deterministic
            FieldWithDataType value;
            value.field = Field::create_field<TYPE_BIGINT>(static_cast<int64_t>(row * 1000 + c));
            value.base_scalar_type_id = PrimitiveType::TYPE_BIGINT;
            value.num_dimensions = 0;
            object.try_emplace(PathInData(path), std::move(value));
        }
        variant_col->try_insert(Field::create_field<TYPE_VARIANT>(std::move(object)));
    }
    variant_col->finalize();
    return variant_col;
}

static ColumnPtr execute_cast(ColumnPtr column, const DataTypePtr& from_type,
                              const DataTypePtr& to_type, FunctionContext* ctx, size_t num_rows) {
    ColumnsWithTypeAndName arguments {{std::move(column), from_type, "src"},
                                      {nullptr, to_type, "dst"}};
    auto function = SimpleFunctionFactory::instance().get_function("CAST", arguments, to_type);
    EXPECT_NE(function, nullptr);
    Block block {arguments};
    size_t result_pos = block.columns();
    block.insert({nullptr, to_type, "result"});
    EXPECT_TRUE(function->execute(ctx, block, {0}, result_pos, num_rows).ok());
    return block.get_by_position(result_pos).column;
}

static ColumnPtr parse_json_strings_to_variant(const ColumnString& json_strings,
                                               const DataTypePtr& variant_type,
                                               bool enforce_max_subcolumns) {
    const auto& variant_data_type = assert_cast<const DataTypeVariant&>(*variant_type);
    auto variant_col = ColumnVariant::create(variant_data_type.variant_max_subcolumns_count());
    auto* variant = assert_cast<ColumnVariant*>(variant_col.get());
    variant->set_variant_enable_typed_paths_to_sparse(
            variant_data_type.variant_enable_typed_paths_to_sparse());
    ParseConfig parse_config;
    variant_util::parse_json_to_variant(*variant, json_strings, parse_config);
    if (enforce_max_subcolumns) {
        EXPECT_TRUE(variant
                            ->adjust_max_subcolumns_count(
                                    variant_data_type.variant_max_subcolumns_count())
                            .ok());
    }
    return ColumnPtr(std::move(variant_col));
}

// variant -> string -> variant when only variant_max_subcolumns_count differs
TEST(FunctionVariantCast, VariantCastViaStringDifferentMaxSubcolumns) {
    const size_t num_rows = 100;
    const size_t num_subcols = 10000;
    auto src_variant_type = std::make_shared<DataTypeVariant>(10000);
    auto dst_variant_type = std::make_shared<DataTypeVariant>(8000);
    auto string_type = std::make_shared<DataTypeString>();
    auto src_col = build_dense_variant_column(num_rows, num_subcols, 10000);

    RuntimeState state;
    auto ctx = FunctionContext::create_context(&state, {}, {});

    // variant -> string -> parse JSON to variant (simulate JSON load path)
    auto string_col =
            execute_cast(src_col->get_ptr(), src_variant_type, string_type, ctx.get(), num_rows);
    const auto& json_strings =
            assert_cast<const ColumnString&>(*remove_nullable(string_col));
    auto json_variant = ColumnVariant::create(dst_variant_type->variant_max_subcolumns_count());
    auto* json_variant_col = assert_cast<ColumnVariant*>(json_variant.get());
    json_variant_col->set_variant_enable_typed_paths_to_sparse(
            dst_variant_type->variant_enable_typed_paths_to_sparse());
    ParseConfig parse_config;
    variant_util::parse_json_to_variant(*json_variant_col, json_strings, parse_config);
    ColumnPtr via_string_variant = std::move(json_variant);
    ASSERT_EQ(via_string_variant->size(), num_rows);

    // Validate by casting to string and comparing with the original string output
    auto via_string_to_str = assert_cast<const ColumnString&>(
            *remove_nullable(execute_cast(via_string_variant, dst_variant_type, string_type,
                                          ctx.get(), num_rows)));
    auto orig_to_str =
            assert_cast<const ColumnString&>(*remove_nullable(string_col));

    ASSERT_EQ(via_string_to_str.size(), orig_to_str.size());
    for (size_t i = 0; i < num_rows; ++i) {
        ASSERT_EQ(via_string_to_str.get_data_at(i).to_string(),
                  orig_to_str.get_data_at(i).to_string());
    }

    const auto& via_string_variant_col =
            assert_cast<const ColumnVariant&>(*via_string_variant);
    ASSERT_FALSE(via_string_variant_col.is_scalar_variant());
    ASSERT_TRUE(via_string_variant_col.has_subcolumn(PathInData("obj0.field0")));
}

// Direct variant -> variant when only variant_max_subcolumns_count differs
TEST(FunctionVariantCast, VariantCastDirectDifferentMaxSubcolumns) {
    const size_t num_rows = 100;
    const size_t num_subcols = 10000;
    auto src_variant_type = std::make_shared<DataTypeVariant>(10000);
    auto dst_variant_type = std::make_shared<DataTypeVariant>(8000);
    auto string_type = std::make_shared<DataTypeString>();
    auto src_col = build_dense_variant_column(num_rows, num_subcols, 10000);

    RuntimeState state;
    auto ctx = FunctionContext::create_context(&state, {}, {});

    auto orig_to_str = assert_cast<const ColumnString&>(
            *remove_nullable(execute_cast(src_col->get_ptr(), src_variant_type, string_type,
                                          ctx.get(), num_rows)));

    auto direct_variant = remove_nullable(
            execute_cast(std::move(src_col), src_variant_type, dst_variant_type, ctx.get(),
                         num_rows));
    ASSERT_EQ(direct_variant->size(), num_rows);

    // Validate by comparing string serialization with original
    auto direct_to_str = assert_cast<const ColumnString&>(
            *remove_nullable(execute_cast(direct_variant, dst_variant_type, string_type,
                                          ctx.get(), num_rows)));
    ASSERT_EQ(direct_to_str.size(), orig_to_str.size());
    for (size_t i = 0; i < num_rows; ++i) {
        ASSERT_EQ(direct_to_str.get_data_at(i).to_string(),
                  orig_to_str.get_data_at(i).to_string());
    }

    const auto& direct_variant_col = assert_cast<const ColumnVariant&>(*direct_variant);
    ASSERT_FALSE(direct_variant_col.is_scalar_variant());
    ASSERT_TRUE(direct_variant_col.has_subcolumn(PathInData("obj0.field0")));
}

TEST(FunctionVariantCast, VariantCastViaStringSqlLikeDifferentMaxSubcolumns) {
    const size_t num_rows = 100;
    const size_t num_subcols = 10000;
    auto src_variant_type = std::make_shared<DataTypeVariant>(10000, false);
    auto dst_variant_type = std::make_shared<DataTypeVariant>(8000, false);
    auto string_type = std::make_shared<DataTypeString>();
    auto seed_variant = build_dense_variant_column(num_rows, num_subcols, 10000);

    RuntimeState state;
    auto ctx = FunctionContext::create_context(&state, {}, {});

    auto seed_string_col = execute_cast(seed_variant->get_ptr(), src_variant_type, string_type,
                                        ctx.get(), num_rows);
    const auto& seed_json_strings =
            assert_cast<const ColumnString&>(*remove_nullable(seed_string_col));
    auto t1_variant = parse_json_strings_to_variant(seed_json_strings, src_variant_type, false);

    auto k_col = ColumnInt32::create();
    for (size_t i = 0; i < num_rows; ++i) {
        k_col->insert(Field::create_field<TYPE_INT>(static_cast<int32_t>(i + 1)));
    }
    auto k_type = std::make_shared<DataTypeInt32>();
    Block src_block;
    src_block.insert({k_col->get_ptr(), k_type, "k"});
    src_block.insert({t1_variant, src_variant_type, "v"});

    auto string_col = execute_cast(src_block.get_by_position(1).column, src_variant_type,
                                   string_type, ctx.get(), num_rows);
    const auto& json_strings =
            assert_cast<const ColumnString&>(*remove_nullable(string_col));
    auto dst_variant = parse_json_strings_to_variant(json_strings, dst_variant_type, false);

    Block dst_block;
    dst_block.insert({src_block.get_by_position(0).column, src_block.get_by_position(0).type,
                      "k"});
    dst_block.insert({dst_variant, dst_variant_type, "v"});

    auto via_to_str = assert_cast<const ColumnString&>(
            *remove_nullable(execute_cast(dst_block.get_by_position(1).column, dst_variant_type,
                                          string_type, ctx.get(), num_rows)));
    auto orig_to_str =
            assert_cast<const ColumnString&>(*remove_nullable(string_col));
    ASSERT_EQ(via_to_str.size(), orig_to_str.size());
    for (size_t i = 0; i < num_rows; ++i) {
        ASSERT_EQ(via_to_str.get_data_at(i).to_string(),
                  orig_to_str.get_data_at(i).to_string());
    }

    const auto& via_variant_col = assert_cast<const ColumnVariant&>(
            *dst_block.get_by_position(1).column);
    ASSERT_FALSE(via_variant_col.is_scalar_variant());
    ASSERT_TRUE(via_variant_col.has_subcolumn(PathInData("obj0.field0")));
}

TEST(FunctionVariantCast, VariantCastDirectSqlLikeDifferentMaxSubcolumns) {
    const size_t num_rows = 100;
    const size_t num_subcols = 10000;
    auto src_variant_type = std::make_shared<DataTypeVariant>(10000, false);
    auto dst_variant_type = std::make_shared<DataTypeVariant>(8000, false);
    auto string_type = std::make_shared<DataTypeString>();
    auto seed_variant = build_dense_variant_column(num_rows, num_subcols, 10000);

    RuntimeState state;
    auto ctx = FunctionContext::create_context(&state, {}, {});

    auto seed_string_col = execute_cast(seed_variant->get_ptr(), src_variant_type, string_type,
                                        ctx.get(), num_rows);
    const auto& seed_json_strings =
            assert_cast<const ColumnString&>(*remove_nullable(seed_string_col));
    auto t1_variant = parse_json_strings_to_variant(seed_json_strings, src_variant_type, true);

    auto k_col = ColumnInt32::create();
    for (size_t i = 0; i < num_rows; ++i) {
        k_col->insert(Field::create_field<TYPE_INT>(static_cast<int32_t>(i + 1)));
    }
    auto k_type = std::make_shared<DataTypeInt32>();
    Block src_block;
    src_block.insert({k_col->get_ptr(), k_type, "k"});
    src_block.insert({t1_variant, src_variant_type, "v"});

    auto orig_to_str = assert_cast<const ColumnString&>(
            *remove_nullable(execute_cast(src_block.get_by_position(1).column, src_variant_type,
                                          string_type, ctx.get(), num_rows)));
    auto direct_variant = remove_nullable(
            execute_cast(src_block.get_by_position(1).column, src_variant_type, dst_variant_type,
                         ctx.get(), num_rows));

    Block dst_block;
    dst_block.insert({src_block.get_by_position(0).column, src_block.get_by_position(0).type,
                      "k"});
    dst_block.insert({direct_variant, dst_variant_type, "v"});

    auto direct_to_str = assert_cast<const ColumnString&>(
            *remove_nullable(execute_cast(dst_block.get_by_position(1).column, dst_variant_type,
                                          string_type, ctx.get(), num_rows)));
    ASSERT_EQ(direct_to_str.size(), orig_to_str.size());
    for (size_t i = 0; i < num_rows; ++i) {
        ASSERT_EQ(direct_to_str.get_data_at(i).to_string(),
                  orig_to_str.get_data_at(i).to_string());
    }

    const auto& direct_variant_col = assert_cast<const ColumnVariant&>(
            *dst_block.get_by_position(1).column);
    ASSERT_FALSE(direct_variant_col.is_scalar_variant());
    ASSERT_TRUE(direct_variant_col.has_subcolumn(PathInData("obj0.field0")));
}

} // namespace doris::vectorized
