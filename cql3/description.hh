/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sstring.hh>

#include "bytes_fwd.hh"

#include <vector>

using namespace seastar;

namespace cql3 {

/// Type representing an entity that can be restored by performing
/// a SINGLE CQL query. It can correspond to a tangible object such as
/// a keyspace, a table, or a role, as well as to a more abstract concept
/// like a role grant.
///
/// Instances of this type correspond to the output of `DESCRIBE` statements.
///
/// ! Important note: !
/// -------------------
/// `description` does NOT perform any additional formatting. As a result,
/// code creating instances of it IS REQUIRED to make sure all of the fields
/// represent valid and correct identifiers/CQL statement. That, for example,
/// encompasses wrapping an identifier with quotation marks if it contains
/// a whitespace character, an uppercase letter, or a quotation mark. Other edge
/// cases may also be possible. The user of this interface is responsible for
/// making sure they take into considerations all relevant circumstances and test
/// their implementation.
/// -------------------
///
/// See scylladb/scylladb#11106 and scylladb/scylladb#18750 for more context.
struct description {
    /// The name of the keyspace the entity belongs to.
    /// Empty optional if and only if the entity does not belong to any keyspace.
    std::optional<sstring> keyspace;
    /// The name of the type of an entity, e.g. a role may be of type: role
    sstring type;
    /// The name of the entity itself, e.g. a keyspace of name `ks` will be of name: ks
    sstring name;
    /// CQL statement that can be used to restore the entity.
    /// Empty in special cases.
    std::optional<sstring> create_statement;

    /// Serialize the description to represent multiple UTF-8 columns following the logic:
    ///
    /// 1. Is `keyspace.has_value()` true?
    ///     Yes: Serialize it.
    ///     No: Serialize a null.
    /// 2. Serialize `type`.
    /// 3. Serialize `name`.
    /// 4. Is `create_statement.has_value()` true?
    ///     Yes: Serialize it.
    ///     No: Don't serialize it.
    ///
    /// As a consequence, the result will represent either a three- or four-column row.
    std::vector<bytes_opt> serialize() const;
};

} // namespace cql3
