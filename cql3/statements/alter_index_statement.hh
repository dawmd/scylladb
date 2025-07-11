/*
 * Copyright (C) 2025-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "cql3/statements/cf_prop_defs.hh"
#include "cql3/statements/schema_altering_statement.hh"
#include "mutation/mutation.hh"
#include "service/client_state.hh"
#include "timestamp.hh"

namespace cql3 {

class query_processor;
class query_options;

namespace statements {

class alter_index_statement : public schema_altering_statement {
private:
    cf_prop_defs _properties;
public:
    alter_index_statement(cf_name, cf_prop_defs);

    virtual future<> check_access(query_processor&, const service::client_state&) const override;
    virtual void validate(query_processor&, const service::client_state&) const override;

    future<std::tuple<seastar::shared_ptr<cql_transport::event::schema_change>, std::vector<mutation>, cql_warnings_vec>>
    prepare_schema_mutations(query_processor&, const query_options&, api::timestamp_type) const override;

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database, cql_stats&) override;
};

} // namespace cql3
} // namespace statements
