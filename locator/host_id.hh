/*
 * Copyright (C) 2015-present ScyllaDB
 *
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include "utils/UUID.hh"

#include <concepts>
#include <ranges>
#include <type_traits>

namespace locator {

using host_id = utils::tagged_uuid<struct host_id_tag>;

template<typename Range>
concept host_id_range =
        std::ranges::range<Range> &&
        std::same_as<std::ranges::range_value_t<Range>, host_id>;

}

