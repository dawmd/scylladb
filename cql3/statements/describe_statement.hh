/*
 * Copyright (C) 2022-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#pragma once

#include <seastar/core/sstring.hh>
#include "cql3/cql_statement.hh"
#include "cql3/statements/raw/describe_statement.hh"

/**
 *  SERVER-SIDE DESCRIBE STATEMENT
 *
 *  Classes below are responsible for executing describe statement.
 *  - cluster_describe_statement - DESC CLUSTER
 *  - schema_describe_statement - DESC [FULL] SCHEMA / DESC [ONLY] KEYSPACE 
 *  - listing_describe_statement - DESC KEYSPACES/TYPES/FUNCTIONS/AGGREGATES/TABLES
 *  - element_describe_statement - DESC TYPE/FUNCTION/AGGREGATE/MATERIALIZED VIEW/INDEX/TABLE
 *  - generic_describe_statements - DESC
 *
 *  Keyspace element means: UDT, UDF, UDA, index, view or table
 *  (see `data_dictionary/keyspace_element.hh`)
 */

namespace replica {

class database;

}

namespace cql3 {

class query_processor;

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

    std::vector<bytes_opt> serialize() const;
};

namespace statements {

using element_type = raw::describe_statement::element_type;

class describe_statement : public cql_statement {
protected:
    describe_statement();

    virtual std::vector<lw_shared_ptr<column_specification>> get_column_specifications() const = 0;
    virtual std::vector<lw_shared_ptr<column_specification>> get_column_specifications(replica::database& db, const service::client_state& client_state) const {
        return get_column_specifications();
    }
    virtual seastar::future<std::vector<std::vector<bytes_opt>>> describe(cql3::query_processor& qp, const service::client_state& client_state) const = 0;
public:
    virtual uint32_t get_bound_terms() const override;
    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;
    virtual seastar::future<> check_access(query_processor& qp, const service::client_state& state) const override;
    virtual seastar::shared_ptr<const metadata> get_result_metadata() const override;

    virtual seastar::future<seastar::shared_ptr<cql_transport::messages::result_message>>
    execute(cql3::query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const override;
};

class cluster_describe_statement : public describe_statement {
private:
    bool should_add_range_ownership(replica::database& db, const service::client_state& client_state) const;
    future<bytes_opt> range_ownership(const service::storage_proxy& proxy, const sstring& ks) const;

protected:
    virtual std::vector<lw_shared_ptr<column_specification>> get_column_specifications() const override;
    virtual std::vector<lw_shared_ptr<column_specification>> get_column_specifications(replica::database& db, const service::client_state& client_state) const override;
    virtual seastar::future<std::vector<std::vector<bytes_opt>>> describe(cql3::query_processor& qp, const service::client_state& client_state) const override;

public:
    cluster_describe_statement();
};

class schema_describe_statement : public describe_statement {
    struct schema_desc {
        bool full_schema;
        bool with_salted_hashes;
    };
    struct keyspace_desc {
        std::optional<sstring> keyspace;
        bool only_keyspace;
    };

private:
    std::variant<schema_desc, keyspace_desc> _config;
    [[maybe_unused]] bool _with_internals;

protected:
    virtual std::vector<lw_shared_ptr<column_specification>> get_column_specifications() const override;
    virtual seastar::future<std::vector<std::vector<bytes_opt>>> describe(cql3::query_processor& qp, const service::client_state& client_state) const override;

public:
    schema_describe_statement(bool full_schema, bool with_salted_hashes, bool with_internals);
    schema_describe_statement(std::optional<sstring> keyspace, bool only, bool with_internals);
};

class listing_describe_statement : public describe_statement {
private:
    element_type _element;
    [[maybe_unused]] bool _with_internals;

protected:
    virtual std::vector<lw_shared_ptr<column_specification>> get_column_specifications() const override;
    virtual seastar::future<std::vector<std::vector<bytes_opt>>> describe(cql3::query_processor& qp, const service::client_state& client_state) const override;

public:
    listing_describe_statement(element_type element, bool with_internals);
    listing_describe_statement(const listing_describe_statement&) = default;
};

class element_describe_statement : public describe_statement {
private:
    element_type _element;
    std::optional<sstring> _keyspace;
    sstring _name;
    [[maybe_unused]] bool _with_internals;

protected:
    virtual std::vector<lw_shared_ptr<column_specification>> get_column_specifications() const override;
    virtual seastar::future<std::vector<std::vector<bytes_opt>>> describe(cql3::query_processor& qp, const service::client_state& client_state) const override;

public:
    element_describe_statement(element_type element, std::optional<sstring> keyspace, sstring name, bool with_internals);
};

class generic_describe_statement : public describe_statement {
private:
    std::optional<sstring> _keyspace;
    sstring _name;
    [[maybe_unused]] bool _with_internals;

protected:
    virtual std::vector<lw_shared_ptr<column_specification>> get_column_specifications() const override;
    virtual seastar::future<std::vector<std::vector<bytes_opt>>> describe(cql3::query_processor& qp, const service::client_state& client_state) const override;

public:
    generic_describe_statement(std::optional<sstring> keyspace, sstring name, bool with_internals);
};

}

}
