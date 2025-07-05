/*
 * Copyright (C) 2025-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include <string_view>

namespace db::view {

bool is_view_option(std::string_view option);

} // namespace db::view
