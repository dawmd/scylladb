/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "cql3/description.hh"

#include "bytes.hh"
#include "types/types.hh"

namespace cql3 {

std::vector<bytes_opt> description::serialize() const {
    std::vector<bytes_opt> result{};
    result.reserve(4);

    if (keyspace) {
        result.push_back(to_bytes(*keyspace));
    } else {
        result.push_back(data_value::make_null(utf8_type).serialize());
    }

    result.push_back(to_bytes(type));
    result.push_back(to_bytes(name));

    if (create_statement) {
        result.push_back({to_bytes(*create_statement)});
    }

    return result;
}

} // namespace cql3
