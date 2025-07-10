/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.0 and Apache-2.0)
 */

#include "cql3/statements/cf_prop_defs.hh"
#include "compaction/compaction_strategy_type.hh"
#include "cql3/statements/request_validations.hh"
#include "data_dictionary/data_dictionary.hh"
#include "db/extensions.hh"
#include "db/tags/extension.hh"
#include "cdc/log.hh"
#include "cdc/cdc_extension.hh"
#include "gms/feature.hh"
#include "gms/feature_service.hh"
#include "tombstone_gc_extension.hh"
#include "tombstone_gc.hh"
#include "db/per_partition_rate_limit_extension.hh"
#include "db/per_partition_rate_limit_options.hh"
#include "db/tablet_options.hh"
#include "utils/bloom_calculations.hh"
#include "db/config.hh"
#include "utils/from_chars_exactly.hh"
#include "utils/fundamental_types.hh"

#include <boost/algorithm/string/predicate.hpp>
#include <variant>

namespace cql3 {

namespace statements {

const sstring cf_prop_defs::KW_COMMENT = "comment";
const sstring cf_prop_defs::KW_GCGRACESECONDS = "gc_grace_seconds";
const sstring cf_prop_defs::KW_PAXOSGRACESECONDS = "paxos_grace_seconds";
const sstring cf_prop_defs::KW_MINCOMPACTIONTHRESHOLD = "min_threshold";
const sstring cf_prop_defs::KW_MAXCOMPACTIONTHRESHOLD = "max_threshold";
const sstring cf_prop_defs::KW_CACHING = "caching";
const sstring cf_prop_defs::KW_DEFAULT_TIME_TO_LIVE = "default_time_to_live";
const sstring cf_prop_defs::KW_MIN_INDEX_INTERVAL = "min_index_interval";
const sstring cf_prop_defs::KW_MAX_INDEX_INTERVAL = "max_index_interval";
const sstring cf_prop_defs::KW_SPECULATIVE_RETRY = "speculative_retry";
const sstring cf_prop_defs::KW_BF_FP_CHANCE = "bloom_filter_fp_chance";
const sstring cf_prop_defs::KW_MEMTABLE_FLUSH_PERIOD = "memtable_flush_period_in_ms";
const sstring cf_prop_defs::KW_SYNCHRONOUS_UPDATES = "synchronous_updates";

const sstring cf_prop_defs::KW_COMPACTION = "compaction";
const sstring cf_prop_defs::KW_COMPRESSION = "compression";
const sstring cf_prop_defs::KW_CRC_CHECK_CHANCE = "crc_check_chance";

const sstring cf_prop_defs::KW_ID = "id";

const sstring cf_prop_defs::COMPACTION_STRATEGY_CLASS_KEY = "class";

const sstring cf_prop_defs::COMPACTION_ENABLED_KEY = "enabled";

const sstring cf_prop_defs::KW_TABLETS = "tablets";

schema::extensions_map cf_prop_defs::make_schema_extensions(const db::extensions& exts) const {
    schema::extensions_map er;
    for (auto& p : exts.schema_extensions()) {
        auto i = _properties.find(p.first);
        if (i != _properties.end()) {
            std::visit([&](auto& v) {
                auto ep = p.second(v);
                if (ep) {
                    er.emplace(p.first, std::move(ep));
                }
            }, i->second);
        }
    }
    return er;
}

data_dictionary::keyspace cf_prop_defs::find_keyspace(const data_dictionary::database db, std::string_view ks_name) {
    try {
        return db.find_keyspace(ks_name);
    } catch (const data_dictionary::no_such_keyspace& e) {
        throw request_validations::invalid_request("{}", e.what());
    }
}

void cf_prop_defs::validate(const data_dictionary::database db, sstring ks_name, const schema::extensions_map& schema_extensions) const {
    // Skip validation if the comapction strategy class is already set as it means we've already
    // prepared (and redoing it would set strategyClass back to null, which we don't want)
    if (_compaction_strategy_class) {
        return;
    }

    const auto& ks = find_keyspace(db, ks_name);

    static std::set<sstring> keywords({
        KW_COMMENT,
        KW_GCGRACESECONDS, KW_CACHING, KW_DEFAULT_TIME_TO_LIVE,
        KW_MIN_INDEX_INTERVAL, KW_MAX_INDEX_INTERVAL, KW_SPECULATIVE_RETRY,
        KW_BF_FP_CHANCE, KW_MEMTABLE_FLUSH_PERIOD, KW_COMPACTION,
        KW_COMPRESSION, KW_CRC_CHECK_CHANCE,  KW_ID, KW_PAXOSGRACESECONDS,
        KW_SYNCHRONOUS_UPDATES, KW_TABLETS,
    });
    static std::set<sstring> obsolete_keywords({
        sstring("index_interval"),
        sstring("replicate_on_write"),
        sstring("populate_io_cache_on_flush"),
        sstring("read_repair_chance"),
        sstring("dclocal_read_repair_chance"),
    });

    const auto& exts = db.extensions();
    property_definitions::validate(keywords, exts.schema_extension_keywords(), obsolete_keywords);

    try {
        get_id();
    } catch(...) {
        std::throw_with_nested(exceptions::configuration_exception("Invalid table id"));
    }

    auto compaction_type_options = get_compaction_type_options();
    if (!compaction_type_options.empty()) {
        auto strategy = compaction_type_options.find(COMPACTION_STRATEGY_CLASS_KEY);
        if (strategy == compaction_type_options.end()) {
            throw exceptions::configuration_exception(sstring("Missing sub-option '") + COMPACTION_STRATEGY_CLASS_KEY + "' for the '" + KW_COMPACTION + "' option.");
        }
        _compaction_strategy_class = sstables::compaction_strategy::type(strategy->second);
        remove_from_map_if_exists(KW_COMPACTION, COMPACTION_STRATEGY_CLASS_KEY);

#if 0
       CFMetaData.validateCompactionOptions(compactionStrategyClass, compactionOptions);
#endif
    }

    auto compression_options = get_compression_options();
    if (compression_options && !compression_options->empty()) {
        auto sstable_compression_class = compression_options->find(sstring(compression_parameters::SSTABLE_COMPRESSION));
        if (sstable_compression_class == compression_options->end()) {
            throw exceptions::configuration_exception(sstring("Missing sub-option '") + compression_parameters::SSTABLE_COMPRESSION + "' for the '" + KW_COMPRESSION + "' option.");
        }
        compression_parameters cp(*compression_options);
        cp.validate(
            compression_parameters::dicts_feature_enabled(bool(db.features().sstable_compression_dicts)),
            compression_parameters::dicts_usage_allowed(db.get_config().sstable_compression_dictionaries_allow_in_ddl()));
    }

    auto per_partition_rate_limit_options = get_per_partition_rate_limit_options(schema_extensions);
    if (per_partition_rate_limit_options && !db.features().typed_errors_in_read_rpc) {
        throw exceptions::configuration_exception("Per-partition rate limit is not supported yet by the whole cluster");
    }

    auto tombstone_gc_options = get_tombstone_gc_options(schema_extensions);
    validate_tombstone_gc_options(tombstone_gc_options, db, ks_name);

    validate_minimum_int(KW_DEFAULT_TIME_TO_LIVE, 0, DEFAULT_DEFAULT_TIME_TO_LIVE);
    validate_minimum_int(KW_PAXOSGRACESECONDS, 0, DEFAULT_GC_GRACE_SECONDS);

    auto min_index_interval = get_int(KW_MIN_INDEX_INTERVAL, DEFAULT_MIN_INDEX_INTERVAL);
    auto max_index_interval = get_int(KW_MAX_INDEX_INTERVAL, DEFAULT_MAX_INDEX_INTERVAL);
    if (min_index_interval < 1) {
        throw exceptions::configuration_exception(KW_MIN_INDEX_INTERVAL + " must be greater than 0");
    }
    if (max_index_interval < min_index_interval) {
        throw exceptions::configuration_exception(KW_MAX_INDEX_INTERVAL + " must be greater than " + KW_MIN_INDEX_INTERVAL);
    }

    if (get_simple(KW_BF_FP_CHANCE)) {
        double bloom_filter_fp_chance = get_double(KW_BF_FP_CHANCE, 0/*not used*/);
        double min_bloom_filter_fp_chance = utils::bloom_calculations::min_supported_bloom_filter_fp_chance();
        if (bloom_filter_fp_chance <= min_bloom_filter_fp_chance || bloom_filter_fp_chance > 1.0) {
            throw exceptions::configuration_exception(format(
                "{} must be larger than {} and less than or equal to 1.0 (got {})",
                KW_BF_FP_CHANCE, min_bloom_filter_fp_chance, bloom_filter_fp_chance));
        }
    }

    auto memtable_flush_period = get_int(KW_MEMTABLE_FLUSH_PERIOD, DEFAULT_MEMTABLE_FLUSH_PERIOD);
    if (memtable_flush_period != 0 && memtable_flush_period < DEFAULT_MEMTABLE_FLUSH_PERIOD_MIN_VALUE) {
        throw exceptions::configuration_exception(format(
            "{} must be 0 or greater than {}",
            KW_MEMTABLE_FLUSH_PERIOD, DEFAULT_MEMTABLE_FLUSH_PERIOD_MIN_VALUE));
    }

    speculative_retry::from_sstring(get_string(KW_SPECULATIVE_RETRY, speculative_retry(speculative_retry::type::NONE, 0).to_sstring()));

    if (auto tablet_options_map = get_tablet_options()) {
        if (!ks.uses_tablets()) {
            throw exceptions::configuration_exception("tablet options cannot be used when tablets are disabled for the keyspace");
        }
        if (!db.features().tablet_options) {
            throw exceptions::configuration_exception("tablet options cannot be used until all nodes in the cluster enable this feature");
        }
        db::tablet_options::validate(*tablet_options_map);
    }
}

std::map<sstring, sstring> cf_prop_defs::get_compaction_type_options() const {
    auto compaction_type_options = get_map(KW_COMPACTION);
    if (compaction_type_options ) {
        return compaction_type_options.value();
    }
    return std::map<sstring, sstring>{};
}

std::optional<std::map<sstring, sstring>> cf_prop_defs::get_compression_options() const {
    auto compression_options = get_map(KW_COMPRESSION);
    if (compression_options) {
        return { compression_options.value() };
    }
    return { };
}

int32_t cf_prop_defs::get_default_time_to_live() const
{
    return get_int(KW_DEFAULT_TIME_TO_LIVE, 0);
}

int32_t cf_prop_defs::get_gc_grace_seconds() const
{
    return get_int(KW_GCGRACESECONDS, DEFAULT_GC_GRACE_SECONDS);
}

bool cf_prop_defs::get_synchronous_updates_flag() const {
    return get_boolean(KW_SYNCHRONOUS_UPDATES, false);
}

int32_t cf_prop_defs::get_paxos_grace_seconds() const {
    return get_int(KW_PAXOSGRACESECONDS, DEFAULT_GC_GRACE_SECONDS);
}

std::optional<table_id> cf_prop_defs::get_id() const {
    auto id = get_simple(KW_ID);
    if (id) {
        return std::make_optional<table_id>(utils::UUID(*id));
    }

    return std::nullopt;
}

std::optional<caching_options> cf_prop_defs::get_caching_options() const {
    auto value = get(KW_CACHING);
    if (!value) {
        return {};
    }
    return std::visit(make_visitor(
        [] (const property_definitions::map_type& map) {
            return map.empty() ? std::nullopt : std::optional<caching_options>(caching_options::from_map(map));
        },
        [] (const sstring& str) {
            return std::optional<caching_options>(caching_options::from_sstring(str));
        }
    ), *value);
}

const cdc::options* cf_prop_defs::get_cdc_options(const schema::extensions_map& schema_exts) const {
    auto it = schema_exts.find(cdc::cdc_extension::NAME);
    if (it == schema_exts.end()) {
        return nullptr;
    }

    auto cdc_ext = dynamic_pointer_cast<cdc::cdc_extension>(it->second);
    return &cdc_ext->get_options();
}

const tombstone_gc_options* cf_prop_defs::get_tombstone_gc_options(const schema::extensions_map& schema_exts) const {
    auto it = schema_exts.find(tombstone_gc_extension::NAME);
    if (it == schema_exts.end()) {
        return nullptr;
    }

    auto ext = dynamic_pointer_cast<tombstone_gc_extension>(it->second);
    return &ext->get_options();
}

const db::per_partition_rate_limit_options* cf_prop_defs::get_per_partition_rate_limit_options(const schema::extensions_map& schema_exts) const {
    auto it = schema_exts.find(db::per_partition_rate_limit_extension::NAME);
    if (it == schema_exts.end()) {
        return nullptr;
    }

    auto ext = dynamic_pointer_cast<db::per_partition_rate_limit_extension>(it->second);
    return &ext->get_options();
}

std::optional<db::tablet_options::map_type> cf_prop_defs::get_tablet_options() const {
    if (auto tablet_options = get_map(KW_TABLETS)) {
        return tablet_options.value();
    }
    return std::nullopt;
}

void cf_prop_defs::apply_to_builder(schema_builder& builder, schema::extensions_map schema_extensions, const data_dictionary::database& db, sstring ks_name) const {
    if (has_property(KW_COMMENT)) {
        builder.set_comment(get_string(KW_COMMENT, ""));
    }

    if (has_property(KW_GCGRACESECONDS)) {
        builder.set_gc_grace_seconds(get_int(KW_GCGRACESECONDS, builder.get_gc_grace_seconds()));
    }

    if (has_property(KW_PAXOSGRACESECONDS)) {
        builder.set_paxos_grace_seconds(get_paxos_grace_seconds());
    }

    std::optional<sstring> tmp_value = {};
    if (has_property(KW_COMPACTION)) {
        if (get_compaction_type_options().contains(KW_MINCOMPACTIONTHRESHOLD)) {
            tmp_value = get_compaction_type_options().at(KW_MINCOMPACTIONTHRESHOLD);
        }
    }
    int min_compaction_threshold = to_int(KW_MINCOMPACTIONTHRESHOLD, tmp_value, builder.get_min_compaction_threshold());

    tmp_value = {};
    if (has_property(KW_COMPACTION)) {
        if (get_compaction_type_options().contains(KW_MAXCOMPACTIONTHRESHOLD)) {
            tmp_value = get_compaction_type_options().at(KW_MAXCOMPACTIONTHRESHOLD);
        }
    }
    int max_compaction_threshold = to_int(KW_MAXCOMPACTIONTHRESHOLD, tmp_value, builder.get_max_compaction_threshold());

    if (min_compaction_threshold <= 0 || max_compaction_threshold <= 0)
        throw exceptions::configuration_exception("Disabling compaction by setting compaction thresholds to 0 has been deprecated, set the compaction option 'enabled' to false instead.");
    builder.set_min_compaction_threshold(min_compaction_threshold);
    builder.set_max_compaction_threshold(max_compaction_threshold);

    if (has_property(KW_COMPACTION)) {
        if (get_compaction_type_options().contains(COMPACTION_ENABLED_KEY)) {
            auto enabled = boost::algorithm::iequals(get_compaction_type_options().at(COMPACTION_ENABLED_KEY), "true");
            builder.set_compaction_enabled(enabled);
        }
    }

    if (has_property(KW_DEFAULT_TIME_TO_LIVE)) {
        builder.set_default_time_to_live(gc_clock::duration(get_int(KW_DEFAULT_TIME_TO_LIVE, DEFAULT_DEFAULT_TIME_TO_LIVE)));
    }

    if (has_property(KW_SPECULATIVE_RETRY)) {
        builder.set_speculative_retry(get_string(KW_SPECULATIVE_RETRY, builder.get_speculative_retry().to_sstring()));
    }

    if (has_property(KW_MEMTABLE_FLUSH_PERIOD)) {
        builder.set_memtable_flush_period(get_int(KW_MEMTABLE_FLUSH_PERIOD, builder.get_memtable_flush_period()));
    }

    if (has_property(KW_MIN_INDEX_INTERVAL)) {
        builder.set_min_index_interval(get_int(KW_MIN_INDEX_INTERVAL, builder.get_min_index_interval()));
    }

    if (has_property(KW_MAX_INDEX_INTERVAL)) {
        builder.set_max_index_interval(get_int(KW_MAX_INDEX_INTERVAL, builder.get_max_index_interval()));
    }

    if (_compaction_strategy_class) {
        builder.set_compaction_strategy(*_compaction_strategy_class);
        builder.set_compaction_strategy_options(get_compaction_type_options());
    }

    builder.set_bloom_filter_fp_chance(get_double(KW_BF_FP_CHANCE, builder.get_bloom_filter_fp_chance()));
    auto compression_options = get_compression_options();
    if (compression_options) {
        builder.set_compressor_params(compression_parameters(*compression_options));
    }

    auto caching_options = get_caching_options();
    if (caching_options) {
        builder.set_caching_options(std::move(*caching_options));
    }

    // for extensions that are not altered, keep the old ones
    auto& old_exts = builder.get_extensions();
    for (auto& [key, ext] : old_exts) {
        if (!_properties.count(key)) {
            schema_extensions.emplace(key, ext);
        }
    }
    // Set default tombstone_gc mode.
    if (!schema_extensions.contains(tombstone_gc_extension::NAME)) {
        auto ext = seastar::make_shared<tombstone_gc_extension>(get_default_tombstonesonte_gc_mode(db, ks_name));
        schema_extensions.emplace(tombstone_gc_extension::NAME, std::move(ext));
    }
    builder.set_extensions(std::move(schema_extensions));

    if (has_property(KW_SYNCHRONOUS_UPDATES)) {
        bool is_synchronous = get_synchronous_updates_flag();
        std::map<sstring, sstring> tags_map = {
            {db::SYNCHRONOUS_VIEW_UPDATES_TAG_KEY, is_synchronous ? "true" : "false"}
        };

        builder.add_extension(db::tags_extension::NAME, ::make_shared<db::tags_extension>(tags_map));
    }

    if (auto tablet_options_opt = get_map(KW_TABLETS)) {
        builder.set_tablet_options(std::move(*tablet_options_opt));
    }
}

void cf_prop_defs::validate_minimum_int(const sstring& field, int32_t minimum_value, int32_t default_value) const
{
    auto val = get_int(field, default_value);
    if (val < minimum_value) {
        throw exceptions::configuration_exception(format("{} cannot be smaller than {}, (default {})",
                                                         field, minimum_value, default_value));
    }
}

std::optional<sstables::compaction_strategy_type> cf_prop_defs::get_compaction_strategy_class() const {
    // Unfortunately, in our implementation, the compaction strategy begins
    // stored in the compaction strategy options, and then the validate()
    // functions moves it into _compaction_strategy_class... If we want a
    // function that works either before or after validate(), we need to
    // check both places.
    if (_compaction_strategy_class) {
        return _compaction_strategy_class;
    }
    auto compaction_type_options = get_compaction_type_options();
    auto strategy = compaction_type_options.find(COMPACTION_STRATEGY_CLASS_KEY);
    if (strategy != compaction_type_options.end()) {
        return sstables::compaction_strategy::type(strategy->second);
    }
    return std::nullopt;
}

// -------------------

namespace {

void validate_keywords_pdfs(const pdfs& props) {
    static const std::flat_set<sstring> valid_value_keywords = {};
    static const std::flat_map<sstring, std::flat_set<sstring>> valid_mapping_keywords = {
        {cf_prop_defs::KW_COMMENT, {}},
        {cf_prop_defs::KW_GCGRACESECONDS, {}},
        {cf_prop_defs::KW_CACHING, {}},
        {cf_prop_defs::KW_DEFAULT_TIME_TO_LIVE, {}},
        {cf_prop_defs::KW_MIN_INDEX_INTERVAL, {}},
        {cf_prop_defs::KW_MAX_INDEX_INTERVAL, {}},
        {cf_prop_defs::KW_SPECULATIVE_RETRY, {}},
        {cf_prop_defs::KW_BF_FP_CHANCE, {}},
        {cf_prop_defs::KW_MEMTABLE_FLUSH_PERIOD, {}},
        {cf_prop_defs::KW_COMPACTION, {}},
        {cf_prop_defs::KW_COMPRESSION, {}},
        {cf_prop_defs::KW_CRC_CHECK_CHANCE, {}},
        {cf_prop_defs::KW_ID, {}},
        {cf_prop_defs::KW_PAXOSGRACESECONDS, {}},
        {cf_prop_defs::KW_SYNCHRONOUS_UPDATES, {}},
        {cf_prop_defs::KW_TABLETS, {}},
        // Obsolete keywords.
        {"index_interval", {}},
        {"replicate_on_write", {}},
        {"populate_io_cache_on_flush", {}},
        {"read_repair_chance", {}},
        {"dclocal_read_repair_chance", {}},
    };
    static const std::flat_set<sstring> ignore_mapping_keywords = {};

    // We need to include schema extensions here...

    if (auto result = props.validate_value_keywords(valid_value_keywords); !result) {
        throw std::runtime_error(std::format("Invalid value property: {}", std::string_view(result.error())));
    }
    if (auto result = props.validate_mapping_keywords(valid_mapping_keywords, ignore_mapping_keywords); !result) {
        throw std::runtime_error(std::format("Invalid mapping keyword: {}", std::string_view(result.error())));
    }
}

// BEGIN Validators of values.

std::optional<caching_options> get_caching_options(const pdfs& props, const schema::extensions_map& exts) {
    const auto it = props.mapping_properties.find(cf_prop_defs::KW_CACHING);
    if (it == props.mapping_properties.end()) {
        return std::nullopt;
    }

    const auto& value = it->second;
    return std::visit(make_visitor(
        [] (const property_definitions::map_type& map) {
            return map.empty() ? std::nullopt : std::optional<caching_options>(caching_options::from_map(map));
        },
        [] (const sstring& str) {
            return std::optional<caching_options>(caching_options::from_sstring(str));
        }
    ), value);
}

// FIXME: Does this one make even sense? It doesn't rely on props...
// FIXME: The original function returns a pointer. Why?
std::optional<cdc::options> get_cdc_options(const pdfs& props, const schema::extensions_map& exts) {
    const auto it = exts.find(cdc::cdc_extension::NAME);
    if (it == exts.end()) {
        return std::nullopt;
    }

    auto ext = dynamic_pointer_cast<cdc::cdc_extension>(it->second);
    return ext->get_options();
}

std::optional<sstables::compaction_strategy_type> get_compaction_strategy_class(const pdfs& props, const schema::extensions_map& exts) {
    const auto it = props.mapping_properties.find(cf_prop_defs::KW_COMPACTION);
    if (it == props.mapping_properties.end()) {
        return std::nullopt;
    }

    // Validate the keywords...

    const auto& map = std::get<typename pdfs::map_type>(it->second);
    const auto class_it = map.find(cf_prop_defs::COMPACTION_STRATEGY_CLASS_KEY);
    if (class_it == map.end()) {
        return std::nullopt;
    }

    return sstables::compaction_strategy::type(class_it->second);
}

std::optional<std::flat_map<sstring, sstring>> get_compaction_type_options(const pdfs& props, const schema::extensions_map& exts) {
    const auto it = props.mapping_properties.find(cf_prop_defs::KW_COMPACTION);
    if (it == props.mapping_properties.end()) {
        return std::nullopt;
    }

    // Validat the type...

    const auto& value = std::get<typename pdfs::map_type>(it->second);
    return value | std::ranges::to<std::flat_map<sstring, sstring>>();
}

int32_t get_default_ttl(const pdfs& props, const schema::extensions_map& exts) {
    const auto it = props.mapping_properties.find(cf_prop_defs::KW_DEFAULT_TIME_TO_LIVE);
    if (it == props.mapping_properties.end()) {
        return 0;
    }

    // Validate the type...
    const auto& value = std::get<sstring>(it->second);
    // FIXME: No exceptions thrown?
    return utils::from_chars_exactly<int32_t>(value, [] (auto&&) {});
}

auto get_gc_grace_seconds(const pdfs& props, const schema::extensions_map& exts) {
    const auto it = props.mapping_properties.find(cf_prop_defs::KW_GCGRACESECONDS);
    if (it == props.mapping_properties.end()) {
        return 0;
    }

    // Validate the type...
    const auto& value = std::get<sstring>(it->second);
    // FIXME: No exceptions thrown?
    return utils::from_chars_exactly<int32_t>(value, [] (auto&&) {});
}

std::optional<table_id> get_id(const pdfs& props, const schema::extensions_map& exts) {
    const auto it = props.mapping_properties.find(cf_prop_defs::KW_ID);
    if (it == props.mapping_properties.end()) {
        return std::nullopt;
    }

    const auto& value = it->second;
    if (!std::holds_alternative<sstring>(value)) {
        throw 1; // TODO
    }

    return std::optional<table_id>(utils::UUID(std::get<sstring>(value)));
}

int32_t get_paxos_grace_seconds(const pdfs& props, const schema::extensions_map& exts) {
    const auto it = props.mapping_properties.find(cf_prop_defs::KW_PAXOSGRACESECONDS);
    if (it == props.mapping_properties.end()) {
        return 0;
    }

    // Validate the type...
    const auto& value = std::get<sstring>(it->second);
    // FIXME: No exceptions thrown?
    return utils::from_chars_exactly<int32_t>(value, [] (auto&&) {});
}

// FIXME: Does this one make even sense? It doesn't rely on props...
// FIXME: The original function returns a pointer. Why?
std::optional<db::per_partition_rate_limit_options> get_per_partition_rate_limit_options(const pdfs& props,
        const schema::extensions_map& exts)
{
    const auto it = exts.find(db::per_partition_rate_limit_extension::NAME);
    if (it == exts.end()) {
        return std::nullopt;
    }

    auto ext = dynamic_pointer_cast<db::per_partition_rate_limit_extension>(it->second);
    return ext->get_options();
}

std::optional<bool> get_synchronous_updates(const pdfs& props, const schema::extensions_map& exts) {
    const auto it = props.mapping_properties.find(cf_prop_defs::KW_SYNCHRONOUS_UPDATES);
    if (it == props.mapping_properties.end()) {
        return std::nullopt;
    }

    // Validate the value type...

    const auto& value = std::get<sstring>(it->second);
    const auto result = utils::parse_bool(value);
    if (!result.has_value()) {
        throw 1; // FIXME
    }
    return result.value();
}

std::optional<db::tablet_options::map_type> get_tablet_options(const pdfs& props, const schema::extensions_map& exts) {
    const auto it = props.mapping_properties.find(cf_prop_defs::KW_TABLETS);
    if (it == props.mapping_properties.end()) {
        return std::nullopt;
    }

    // Validate the value...

    const auto& map = std::get<typename pdfs::map_type>(it->second);
    return map | std::ranges::to<db::tablet_options::map_type>();
}

// FIXME: Does this one make even sense? It doesn't rely on props...
// FIXME: The original function returns a pointer. Why?
std::optional<tombstone_gc_options> get_tombstone_gc_options(const pdfs& props, const schema::extensions_map& exts) {
    const auto it = exts.find(tombstone_gc_extension::NAME);
    if (it == exts.end()) {
        return std::nullopt;
    }

    auto ext = dynamic_pointer_cast<tombstone_gc_extension>(it->second);
    return ext->get_options();
}

// END Validators of values.

} // anonymous namespace

cf_pdfs cf_pdfs::from_pdfs(const pdfs& props, const schema::extensions_map& exts) {
    validate_keywords_pdfs(props);

    cf_pdfs result{};

    result.caching_options = get_caching_options(props, exts);
    result.cdc_options = get_cdc_options(props, exts);
    result.compaction_strategy_class = get_compaction_strategy_class(props, exts);
    result.compaction_type_options = get_compaction_type_options(props, exts);
    result.default_ttl = get_default_ttl(props, exts);
    result.gc_grace_seconds = get_gc_grace_seconds(props, exts);
    result.id = get_id(props, exts);
    result.paxos_grace_seconds = get_paxos_grace_seconds(props, exts);
    result.per_partition_rate_limit_options = get_per_partition_rate_limit_options(props, exts);
    result.synchronous_updates = get_synchronous_updates(props, exts);
    result.tablet_options = get_tablet_options(props, exts);
    result.tombstone_gc_options = get_tombstone_gc_options(props, exts);

    return result;
}

}

}
