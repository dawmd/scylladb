/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <concepts>
#include <ostream>
#include <utility>
#include <seastar/core/sstring.hh>

namespace replica {
class database;
}

namespace data_dictionary {

/**
 * `keyspace_element` is a common interface used to describe elements of keyspace.
 * It is used in `describe_statement`.
 *
 * Currently the elements of keyspace are:
 * - keyspace
 * - user-defined types
 * - user-defined functions/aggregates
 * - tables, views and indexes
*/
class keyspace_element {
public:
    virtual seastar::sstring keyspace_name() const = 0;
    virtual seastar::sstring element_name() const = 0;

    // Override one of these element_type() overloads.
    virtual seastar::sstring element_type() const { return ""; }
    virtual seastar::sstring element_type(replica::database& db) const { return element_type(); }

    // Override one of these describe() overloads.
    virtual std::ostream& describe(std::ostream& os) const { return os; }
    virtual std::ostream& describe(replica::database& db, std::ostream& os, bool with_internals) const { return describe(os); }
};

/**
 * Constraint identifying types able to generate multiple descriptions
 * of the same type (sharing the same `keyspace_name` and `element_type`).
 */
template <typename T>
concept keyspace_element_generator = requires (const T& t, const replica::database& db, bool with_internals) {
    /// Types satisfying this concept should represent a set of instances of `keyspace_element`.
    /// As such, `t.keyspace()` and `t.element_type(db)` should have the same result and the same side
    /// effects as if the same methods were called for each individual `keyspace_element` that `t` represents.
    /// However, `T` is allowed to optimize memory consumption, etc. that have no semantic consequences.
    ///
    /// The methods below take `replica::database` as a const-reference because, as explained above,
    /// we want to mimic the behavior of the corresponding methods of `keyspace_element`.
    ///
    /// If `t.element_type(db)` or `t.describe_elements(db, with_internals)` could modify the passed
    /// `replica::database` instance, `t.element_type(db)` might have different semantics from
    /// calling `keyspace_element::element_type` on each individual element represented by `t`
    /// -- which is forbidden. On the other hand, if `t.describe_elements(db, with_internals)` were to
    /// modify the `replica::database` object, it would be reliant on the order in which we iterate
    /// over the elements, which we want to avoid as well.
    ///
    /// Describing keyspace elements should NOT need to modify any object anyway, so we deem it a good decision
    /// to enforce the const-reference.

    { t.keyspace_name() } -> std::same_as<seastar::sstring>;
    { t.element_type(db) } -> std::same_as<seastar::sstring>;

    /// Method used to create the rest of the information necessary to describe a kesypace element
    /// for multiple of them at the same time. The returned type represents a pair `(element_name, create_statement)`.
    { t.describe_elements(db, with_internals) } -> std::ranges::range;
    requires std::same_as<std::ranges::range_value_t<decltype(t.describe_elements(db, with_internals))>,
            std::pair<seastar::sstring, seastar::sstring>>;
};

} // namespace data_dictionary
