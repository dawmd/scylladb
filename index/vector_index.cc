/*
 * Copyright 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */


#include "cql3/statements/index_target.hh"
#include "exceptions/exceptions.hh"
#include "schema/schema.hh"
#include "index/vector_index.hh"
#include "concrete_types.hh"
#include <seastar/core/sstring.hh>


namespace secondary_index {

void vector_index::validate(const schema &schema, const std::vector<::shared_ptr<cql3::statements::index_target>> &targets, const gms::feature_service& fs) {
    if (targets.size() != 1) {
        throw exceptions::invalid_request_exception("Vector index can only be created on a single column");
    }

    auto target = targets[0];
    auto c_def = schema.get_column_definition(to_bytes(target->column_name()));
    if (!c_def) {
        throw exceptions::invalid_request_exception(format("Column {} not found in schema", target->column_name()));
    }
    auto type = c_def->type;
    if (!type->is_vector() || static_cast<const vector_type_impl*>(type.get())->get_elements_type()->get_kind() != abstract_type::kind::float_kind) {
        throw exceptions::invalid_request_exception(format("Vector indexes are only supported on columns of vectors of floats", target->column_name()));
    }
}

std::unique_ptr<secondary_index::custom_index> vector_index_factory() {
    return std::make_unique<vector_index>();
}

}
