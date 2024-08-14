/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "qos_common.hh"
#include "utils/overloaded_functor.hh"
#include "cql3/query_processor.hh"
#include "cql3/result_set.hh"
#include "cql3/untyped_result_set.hh"
#include "utils/small_vector.hh"
#include <string_view>
#include <variant>

namespace qos {

static logging::logger logger("qos");

service_level_options service_level_options::replace_defaults(const service_level_options& default_values) const {
    service_level_options ret = *this;
    std::visit(overloaded_functor {
        [&] (const unset_marker& um) {
            // reset the value to the default one
            ret.timeout = default_values.timeout;
        },
        [&] (const delete_marker& dm) {
            // remove the value
            ret.timeout = unset_marker{};
        },
        [&] (const lowres_clock::duration&) {
            // leave the value as is
        },
    }, ret.timeout);
    switch (ret.workload) {
    case workload_type::unspecified:
        ret.workload = default_values.workload;
        break;
    case workload_type::delete_marker:
        ret.workload = workload_type::unspecified;
        break;
    default:
        // no-op
        break;
    }
    return ret;
}

service_level_options service_level_options::merge_with(const service_level_options& other) const {
    auto maybe_update_timeout_name = [] (service_level_options& slo, const service_level_options& other) {
        if (slo.effective_names && other.effective_names) {
            slo.effective_names->timeout = other.effective_names->timeout;
        }
    };
    auto maybe_update_workload_name = [] (service_level_options& slo, const service_level_options& other) {
        if (slo.effective_names && other.effective_names) {
            slo.effective_names->workload = other.effective_names->workload;
        }
    };

    service_level_options ret = *this;
    std::visit(overloaded_functor {
        [&] (const unset_marker& um) {
            ret.timeout = other.timeout;
            maybe_update_timeout_name(ret, other);
        },
        [&] (const delete_marker& dm) {
            ret.timeout = other.timeout;
            maybe_update_timeout_name(ret, other);
        },
        [&] (const lowres_clock::duration& d) {
            if (auto* other_timeout = std::get_if<lowres_clock::duration>(&other.timeout)) {
                auto prev_timeout = ret.timeout;
                ret.timeout = std::min(d, *other_timeout);

                if (prev_timeout != ret.timeout) {
                    maybe_update_timeout_name(ret, other);
                }
            }
        },
    }, ret.timeout);

    // Specified workloads should be preferred over unspecified ones
    auto prev_workload = ret.workload;
    if (ret.workload == workload_type::unspecified || other.workload == workload_type::unspecified) {
        ret.workload = std::max(ret.workload, other.workload);
    } else {
        ret.workload = std::min(ret.workload, other.workload);
    }
    if (prev_workload != ret.workload) {
        maybe_update_workload_name(ret, other);
    }

    return ret;
}

std::string_view service_level_options::to_string(const workload_type& wt) {
    switch (wt) {
    case workload_type::unspecified: return "unspecified";
    case workload_type::batch: return "batch";
    case workload_type::interactive: return "interactive";
    case workload_type::delete_marker: return "delete_marker";
    }
    abort();
}

std::ostream& operator<<(std::ostream& os, const service_level_options::workload_type& wt) {
    return os << service_level_options::to_string(wt);
}

std::optional<service_level_options::workload_type> service_level_options::parse_workload_type(std::string_view sv) {
    if (sv == "null") {
        return workload_type::unspecified;
    } else if (sv == "interactive") {
        return workload_type::interactive;
    } else if (sv == "batch") {
        return workload_type::batch;
    }
    return std::nullopt;
}

void service_level_options::init_effective_names(std::string_view service_level_name) {
    effective_names = service_level_options::slo_effective_names {
        .timeout = sstring(service_level_name),
        .workload = sstring(service_level_name)
    };
}

service::query_state& qos_query_state() {
    using namespace std::chrono_literals;
    const auto t = 10s;
    static timeout_config tc{ t, t, t, t, t, t, t };
    static thread_local service::client_state cs(service::client_state::internal_tag{}, tc);
    static thread_local service::query_state qs(cs, empty_service_permit());
    return qs;
};

static service_level_options::timeout_type get_duration(const cql3::untyped_result_set_row&row, std::string_view col_name) {
    auto dur_opt = row.get_opt<cql_duration>(col_name);
    if (!dur_opt) {
        return qos::service_level_options::unset_marker{};
    }
    return std::chrono::duration_cast<lowres_clock::duration>(std::chrono::nanoseconds(dur_opt->nanoseconds));
};

future<qos::service_levels_info> get_service_levels(cql3::query_processor& qp, std::string_view ks_name, std::string_view cf_name, db::consistency_level cl) {
    sstring prepared_query = format("SELECT * FROM {}.{};", ks_name, cf_name);
    auto result_set = co_await qp.execute_internal(prepared_query, cl, qos_query_state(), cql3::query_processor::cache_internal::yes);

    qos::service_levels_info service_levels;
    for (auto &&row : *result_set) {
        try {
            auto service_level_name = row.get_as<sstring>("service_level");
            auto workload = qos::service_level_options::parse_workload_type(row.get_opt<sstring>("workload_type").value_or(""));
            qos::service_level_options slo{
                .timeout = get_duration(row, "timeout"),
                .workload = workload.value_or(qos::service_level_options::workload_type::unspecified),
            };
            service_levels.emplace(service_level_name, slo);
        } catch (...) {
            logger.warn("Failed to fetch data for service levels: {}", std::current_exception());
        }
    }

    co_return service_levels;
}

future<service_levels_info> get_service_level(cql3::query_processor& qp, std::string_view ks_name, std::string_view cf_name, sstring service_level_name, db::consistency_level cl) {
    sstring prepared_query = format("SELECT * FROM {}.{} WHERE service_level = ?;", ks_name, cf_name);
    auto result_set = co_await  qp.execute_internal(prepared_query, cl, qos_query_state(), {service_level_name}, cql3::query_processor::cache_internal::yes);

    qos::service_levels_info service_levels;
    if (!result_set->empty()) {
        try {
            auto &&row = result_set->one();
            auto workload = qos::service_level_options::parse_workload_type(row.get_opt<sstring>("workload_type").value_or(""));
            qos::service_level_options slo{
                .timeout = get_duration(row, "timeout"),
                .workload = workload.value_or(qos::service_level_options::workload_type::unspecified),
            };
            service_levels.emplace(service_level_name, slo);
        } catch (...) {
            logger.warn("Failed to fetch data for service level {}: {}", service_level_name, std::current_exception());
        }
    }

    co_return service_levels;
}

sstring service_levels_info::keyspace_name() const {
    return "<null>";
}

sstring service_levels_info::element_type(const replica::database&) const {
    return "service_level";
}

static std::optional<sstring> describe_service_level(std::string_view sl_name, const service_level_options& sl_opts) {
    using slo = service_level_options;

    utils::small_vector<sstring, 2> opts{};

    if (std::holds_alternative<slo::delete_marker>(sl_opts.timeout)) {
        return std::nullopt;
    }
    if (auto maybe_timeout = std::get_if<lowres_clock::duration>(&sl_opts.timeout)) {
        opts.push_back(seastar::format("timeout = {}", *maybe_timeout));
    }

    switch (sl_opts.workload) {
        case slo::workload_type::delete_marker:
            // TODO: Is it possible that this is set to `delete_marker`, but `sl_opts.timeout` is not?
            return std::nullopt;
        case slo::workload_type::batch:
            opts.push_back("workload_type = 'batch'");
            break;
        case slo::workload_type::interactive:
            opts.push_back("workload_type = 'interactive'");
            break;
        case slo::workload_type::unspecified:
            break;
    }

    switch (opts.size()) {
        case 0:
            return seastar::format("CREATE SERVICE_LEVEL {};", sl_name);
        case 1:
            return fmt::format("CREATE SERVICE_LEVEL {} WITH {};", sl_name, opts[0]);
        case 2:
            return fmt::format("CREATE SERVICE LEVEL {} WITH {} AND {};", sl_name, opts[0], opts[1]);
        default:
            on_internal_error(logger, seastar::format("Unexpected number of elements: {}", opts.size()));
    }
}

std::vector<std::pair<sstring, sstring>> service_levels_info::describe_elements(const replica::database&, bool with_internals) const {
    std::vector<std::pair<sstring, sstring>> result{};
    result.reserve(this->size());

    for (const auto& [sl_name, sl_options] : *this) {
        if (auto maybe_desc = describe_service_level(sl_name, sl_options)) {
            result.emplace_back(sl_name, *maybe_desc);
        }
    }

    return result;
}


} // namespace qos
