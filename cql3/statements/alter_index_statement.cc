/*
 * Copyright (C) 2025-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "cql3/statements/alter_index_statement.hh"

#include "cql3/query_processor.hh"
#include "cql3/statements/cf_prop_defs.hh"
#include "cql3/statements/index_prop_defs.hh"
#include "data_dictionary/data_dictionary.hh"
#include "exceptions/exceptions.hh"
#include "index/secondary_index_manager.hh"
#include "replica/database.hh"
#include "service/migration_manager.hh"
#include "validation.hh"

namespace cql3::statements {

namespace {

schema_ptr get_schema_for_index(const data_dictionary::database& db, std::string_view ks, std::string_view index_name) {
    if (!db.has_keyspace(ks)) {
        throw exceptions::keyspace_not_defined_exception(seastar::format(
                "Keyspace {} does not exist", ks));
    }

    schema_ptr s = db.find_indexed_table(ks, index_name);
    if (!s) {
        throw exceptions::invalid_request_exception(seastar::format(
                "Index '{}' does not exist in keyspace {}", index_name, ks));
    }

    return s;
}

view_ptr prepare_view(data_dictionary::database db, const cf_prop_defs& props, std::string_view ks, std::string_view idx) {
    auto schema_exts = props.make_schema_extensions(db.extensions());

    // ...

    auto view_name = secondary_index::index_table_name(sstring(idx));
    schema_ptr view_schema = validation::validate_column_family(db, sstring(ks), view_name);

    schema_builder builder{view_schema};
    props.apply_to_builder(builder, schema_exts, db, sstring(ks));

    if (builder.get_gc_grace_seconds() == 0) {
        throw exceptions::invalid_request_exception(
                "Cannot alter gc_grace_seconds of a materialized view to 0, since this "
                "value is used to TTL undelivered updates. Setting gc_grace_seconds too "
                "low might cause undelivered updates to expire before being replayed.");
    }

    if (builder.default_time_to_live().count() > 0) {
        throw exceptions::invalid_request_exception(
                "Cannot set or alter default_time_to_live for a materialized view. "
                "Data in a materialized view always expire at the same time than "
                "the corresponding data in the parent table.");
    }

    return view_ptr(builder.build());
}

} // anonymous namespace

alter_index_statement::alter_index_statement(cf_name name, cf_prop_defs properties)
    : schema_altering_statement{std::move(name)}
    , _properties(std::move(properties))
{}

future<> alter_index_statement::check_access(query_processor& qp, const service::client_state& state) const {
    schema_ptr s = get_schema_for_index(qp.db(), keyspace(), column_family());
    return state.has_column_family_access(s->ks_name(), s->cf_name(), auth::permission::ALTER);
}

void alter_index_statement::validate(query_processor& qp, const service::client_state& state) const {
    schema_ptr s = get_schema_for_index(qp.db(), keyspace(), column_family());

}

future<std::tuple<seastar::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql_warnings_vec>>
alter_index_statement::prepare_schema_mutations(query_processor& qp, const query_options&, api::timestamp_type ts) const {
    schema_ptr view_schema = prepare_view(qp.db(), _properties, keyspace(), column_family());
    std::vector<mutation> m = co_await service::prepare_view_update_announcement(qp.proxy(), view_ptr(view_schema), ts);

    using namespace cql_transport;
    auto ret = ::make_shared<event::schema_change>(
            event::schema_change::change_type::UPDATED,
            event::schema_change::target_type::TABLE,
            keyspace(),
            column_family());

    co_return std::make_tuple(std::move(ret), std::move(m), std::vector<sstring>());
}

std::unique_ptr<prepared_statement> alter_index_statement::prepare(data_dictionary::database db, cql_stats&) {
    // FIXME: Add CQL stats?
    return std::make_unique<prepared_statement>(audit_info(), seastar::make_shared<alter_index_statement>(*this));
}

} // namespace cql3::statements
