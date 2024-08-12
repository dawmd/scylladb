/*
 * Copyright (C) 2024-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

// Seastar features.
#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

// STD.
#include <utility>
#include <vector>

using namespace seastar;

namespace data_dictionary {

/// Common interface for entities that can be described, i.e. that can be created
/// using CQL statements. It encompasses not only "tangible" entities like roles,
/// but also more abstract ones like role grants.
struct describable_entity {
    virtual ~describable_entity() = default;

    virtual sstring entity_type() const = 0;
    virtual sstring entity_name() const = 0;

    /// Returns a statement that can be used to restore the entity.
    virtual sstring describe(bool with_internals) const = 0;
};

/// Common interface for sets of entities that can be described. Analogous to
/// a set of `describable_entity` where all of them share the same `element_type`.
class description_generator {
private:
    /// Type representing pairs `(entity_name, create_statement)`.
    using description_type = std::pair<sstring, sstring>;

public:
    virtual ~description_generator() = default;

    virtual sstring entity_type() const = 0;
    virtual future<std::vector<description_type>> describe_entities(bool with_internals) const = 0;
};

} // namespace data_dictionary
