/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include "user_function.hh"
#include "cql3/description.hh"
#include "cql3/util.hh"
#include "log.hh"
#include "lang/wasm.hh"

#include <seastar/core/thread.hh>

#include <ranges>

namespace cql3 {
namespace functions {

extern logging::logger log;

user_function::user_function(function_name name, std::vector<data_type> arg_types, std::vector<sstring> arg_names,
        sstring body, sstring language, data_type return_type, bool called_on_null_input, context ctx)
    : abstract_function(std::move(name), std::move(arg_types), std::move(return_type)),
      _arg_names(std::move(arg_names)), _body(std::move(body)), _language(std::move(language)),
      _called_on_null_input(called_on_null_input), _ctx(std::move(ctx)) {}

bool user_function::is_pure() const { return true; }

bool user_function::is_native() const { return false; }

bool user_function::is_aggregate() const { return false; }

bool user_function::requires_thread() const { return true; }

bytes_opt user_function::execute(std::span<const bytes_opt> parameters) {
    const auto& types = arg_types();
    if (parameters.size() != types.size()) {
        throw std::logic_error("Wrong number of parameters");
    }

    if (!seastar::thread::running_in_thread()) {
        on_internal_error(log, "User function cannot be executed in this context");
    }
    for (auto& param : parameters) {
        if (!param && !_called_on_null_input) {
            return std::nullopt;
        }
    }
    return seastar::visit(_ctx,
        [&] (lua_context& ctx) -> bytes_opt {
            std::vector<data_value> values;
            values.reserve(parameters.size());
            for (int i = 0, n = types.size(); i != n; ++i) {
                const data_type& type = types[i];
                const bytes_opt& bytes = parameters[i];
                values.push_back(bytes ? type->deserialize(*bytes) : data_value::make_null(type));
            }
            return lua::run_script(lua::bitcode_view{ctx.bitcode}, values, return_type(), ctx.cfg).get();
        },
        [&] (wasm::context& ctx) -> bytes_opt {
            try {
                return wasm::run_script(name(), ctx, arg_types(), parameters, return_type(), _called_on_null_input).get();
            } catch (const wasm::exception& e) {
                throw exceptions::invalid_request_exception(format("UDF error: {}", e.what()));
            }
        });
}

std::ostream& user_function::describe(std::ostream& os) const {
    auto ks = cql3::util::maybe_quote(name().keyspace);
    auto na = cql3::util::maybe_quote(name().name);

    os << "CREATE FUNCTION " << ks << "." << na << "(";
    for (size_t i = 0; i < _arg_names.size(); i++) {
        if (i > 0) {
            os << ", ";
        }
        os << _arg_names[i] << " " << _arg_types[i]->cql3_type_name();
    }
    os << ")\n";

    if (_called_on_null_input) {
        os << "CALLED";
    } else {
        os << "RETURNS NULL";
    }
    os << " ON NULL INPUT\n"
       << "RETURNS " << _return_type->cql3_type_name() << "\n"
       << "LANGUAGE " << _language << "\n"
       << "AS $$\n"
       << _body << "\n"
       << "$$;";

    return os;
}

description user_function::describe() const {
    using std::literals::string_view_literals::operator""sv;

    auto argument_list_view = std::views::zip_transform([] (std::string_view arg_name, const data_type& type) {
        return seastar::format("{} {}", arg_name, type->cql3_type_name());
    }, _arg_names, _arg_types) | std::views::join_with(", "sv);

    constexpr std::string_view udf_template =
            "CREATE FUNCTION {}.{}({})\n"
            "{} ON NULL INPUT\n"
            "RETURNS {}\n"
            "LANGUAGE {}\n"
            "AS $$\n"
            "{}\n"
            "$$;";

    sstring create_statement = seastar::format(udf_template.data(),
            cql3::util::maybe_quote(name().keyspace), cql3::util::maybe_quote(name().name), argument_list_view,
            _called_on_null_input ? "CALLED"sv : "RETURNS NULL"sv,
            _return_type->cql3_type_name(),
            _language,
            _body);

    return description {
        .keyspace = name().keyspace,
        .type = "function",
        .name = name().name,
        .create_statement = std::move(create_statement)
    };
}

} // namespace functions

} // namespace cql3
