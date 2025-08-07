/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/sstring.hh>

#include "service/qos/qos_common.hh"

#include <map>
#include <set>

using namespace seastar;

namespace auth {

class service;

} // namespace auth

namespace qos {

class service_level_controller;

struct effective_service_level_cache_subscriber {
    virtual ~effective_service_level_cache_subscriber() = default;
    virtual future<> on_effective_service_levels_cache_reloaded() = 0;
};

class effective_service_level_controller : public peering_sharded_service<effective_service_level_controller> {
private:
    service_level_controller& _sl_controller;
    auth::service& _auth_service;

    // Mappings `role name` -> `corresponding service level`.
    std::map<sstring, service_level_options> _mappings;

    std::set<effective_service_level_cache_subscriber*> _subscribers;

public:
    effective_service_level_controller(service_level_controller& sl_controller, auth::service& auth_service)
        : _sl_controller(sl_controller)
        , _auth_service(auth_service)
    {}

    future<std::optional<service_level_options>> find_effective_service_level(const sstring& role_name);
    std::optional<service_level_options> find_cached_effective_service_level(const sstring& role_name);

    service_level_controller& get_service_level_controller() noexcept {
        return _sl_controller;
    }
    const service_level_controller& get_service_level_controller() const noexcept {
        return _sl_controller;
    }

    future<> reload_cache();

    void register_subscriber(effective_service_level_cache_subscriber*);
    future<> unregister_subscriber(effective_service_level_cache_subscriber*);

private:
    future<> notify_effective_service_levels_cache_reloaded();
};

} // namespace qos
