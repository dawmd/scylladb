# Motivation
Scylla clusters keep various kinds of metadata that needs to be global. The list includes but is not limited to:

* schema,
* authorization/authentication,
* service levels.

In the past, they were replicated using mechanisms like custom synchronization RPC, while auth and service levels—distributed tables. All of them suffered from consistency issues, e.g. concurrent modification could make them break, some were prone to data loss if a node was lost. To solve those issues, we replaced the mechanisms with the Raft algorithm and now they're managed by group0.

Since we want to support cluster-wide backup and restore[^1], we need to be able to backup and restore cluster metadata. The currently recommended way of backing-up tables with user data is to take a snapshot, upload the SSTables elswehere and, during restore, download them back to appropriate nodes in the fresh cluster.

Unfortunately, that doesn't work well with Raft-replicated tables for the following reasons:

* group0-managed tables are just local system tables and any modifications to them are propagated to all nodes using Raft. That assumes that if there are no unapplied Raft commands, all nodes will have the same state of the table.

  Theoretically, we could make it work if we restored the same SSTables on all nodes, but although it sounds simple, it's not always possible: Scylla Manager cannot transfer SSTables backed-up in one data center to another one. Taking a snapshot on more than one node is not atomic and does not gurantee that the contents of their snapshots will be consistent. In case of any differences between the nodes, there is no repair algorithm that could reconcile their states. The fact that application of group0 commands to tables is not synchronized with the flush on snapshot doesn't help either.

* Restoring data by putting SSTables into the data directory is potentially unsafe. Nothing prevents the administrator from restoring only some of the backed-up SSTables. Also, group0 generally assumes that all its data was created by relevant code in Scylla and has not been tampered with by the user. It assumes that for all data stored in SSTables, the commitlog, etc.

To summarize, it's very easy to make a mistake here and there is no easy way to fix inconsistencies. Supporting some existing use cases requires awkward workarounds.

We would like there to be an alternative that provides the following properties:

* be simple for an administrator,

* should work even if the schema of the metadata changes in the future, e.g. new parameters are added to service levels,

* be atomic. Group0 changes are linearizable and a backup should produce a description that corresponds to the state of metadata at a specific point—close to the time when the user issued a backup.

# Synopsis of the solution

Because the schema, auth, and service levels can be manipulated by administrators via the CQL interface, we want to generate a sequence of CQL statements that, when applied in order, would restore their current state. The order of those statements is important since there may be depencies between the entities the statements would recreate, e.g. we can only grant a role to another if they already exist. If new dependencies are introduced, the CQL interface will still be usable as long as we update the implementation.

The solution relies on the assumption that such a sequence of statements can be generated from the internal state.

# Implementation of the solution

We introduce a statement responsible for describing schema, auth, and service level entities: `DESC SCHEMA`. It encompasses three "tiers":

* `DESCRIBE [FULL] SCHEMA`: describe elements of the non-system schema: keyspaces, tables, views, UDTs, etc. When `FULL` is used, it also includes the elements of the system schema, e.g. system tables.

* `DESCRIBE [FULL] SCHEMA WITH INTERNALS`: in addition to the output of the previous tier, the statement also describes auth and service levels. The statements corresponding to restoring roles do *not* contain any information about their passwords.

* `DESCRIBE [FULL] SCHEMA WITH INTERNALS AND PASSWORDS`: aside from the information retrieved as part of the previous tier, the statements corresponding to restoring roles *do* contain information about their passwords, i.e. their salted hashes. For more information, see the relevant section below.

Instead of `DESCRIBE`, the user can use its shortened form: `DESC`.

As a result of the query, the user will obtain a set of rows, each of which consists of four values:

* `keyspace_name`: the name of the keyspace the entity is part of,
* `type`: the type of the entity,
* `name`: the name of the entity,
* `create_statement`: the statement used for restoring the entity.

All of the values are always present.

Executing `DESCRIBE [FULL] SCHEMA WITH INTERNALS AND PASSWORDS` requires that the user performing the query be a superuser.

## Restoring process and its side effects

When a resource is created, the current role is always granted full permissions to that resource. As a consequence, the role used to restore the backup will gain permissions to all of the resources that will be recreated.

However, we don't see it as an issue. The role that is used to restore the schema, auth, and service levels *should* be a superuser. Superusers are granted full access to all resources, so unless the superuser status of the role is revoked, there will be no unwanted side effects.

## Restoring roles with passwords

Scylla doesn't store passwords of the roles; instead, it stores salted hashes corresponding to them. When the user wants to log in, they provide a password. Then, that password and so-called "salt" are used to generate a hash. Finally, that hash is compared against the salted hash stored by the database to determine if the provided password is correct.

To restore a role with its password, we want utilize that salted hash. We introduce a new form of the `CREATE ROLE` statement:

```sql
CREATE ROLE [ IF NOT EXSITS ] `role_name` WITH SALTED HASH = '`salted_hash`' ( AND `role_option` )*
```

where

```sql
role_option: LOGIN '=' `string`
           :| SUPERUSER '=' `boolean`
```

Performing that query will result in creating a new role whose salted hash stored in the database is exactly the same as the one provided by the user. If the specified role already exists, the query will fail and no side effect will be observed—in short, we follow the semantics of the "normal" version of `CREATE ROLE`.

The user executing that statement is required to be a superuser.

## List of statements generated by `DESCRIBE SCHEMA` with examples

TODO: Fill in this section.

### Creating roles

```sql
CREATE ROLE nicholas WITH PASSWORD = "okoń" AND LOGIN = true;
CREATE ROLE aardvark;
```

| `keyspace_name` | `type` | `name`   | `create_statement` |
| :-------------: | :----: | :------: | :----------------: |
| ""              | role   | nicholas | `CREATE ROLE nicholas WITH SALTED HASH = '<salted_hash>' AND LOGIN = true AND SUPERUSER = false;` |
| ""              | role   | aardvark | `CREATE ROLE nicholas WITH LOGIN = false AND SUPERUSER = false;` |

#### Notes

The create statement will always contain the options `LOGIN` and `SUPERUSER`. The options are always in the following order: `SALTED HASH`, `LOGIN`, `SUPERUSER`.

In the case of the default superuser, the create statement will use the option `IF NOT EXISTS` to prevent attempting to recreate the role.

### Role grants

```sql
CREATE ROLE nicholas WITH PASSWORD = "okoń" AND LOGIN = true;
CREATE ROLE aardvark;
GRANT aardvark TO nicholas;
```

| `keyspace_name` | `type`       |  `name`  | `create_statement` |
| :-------------: | :----------: | :------: | :----------------: |
| ""              | grant_role   | aardvark | `GRANT aardvark TO nicholas;` |

### Permission grants

```sql
CREATE ROLE nicholas WITH PASSWORD = "okoń" AND LOGIN = true;
GRANT SELECT ON ALL TABLES TO nicholas;
```

| `keyspace_name` | `type`           |  `name`  | `create_statement` |
| :-------------: | :--------------: | :------: | :----------------: |
| ""              | grant_permission | nicholas | `GRANT SELECT ON ALL TABLES TO nicholas;` |

### Creating service levels

```sql
CREATE SERVICE_LEVEL olap WITH SHARES = 100;
```

| `keyspace_name` | `type`        |  `name`  | `create_statement` |
| :-------------: | :-----------: | :------: | :----------------: |
| ""              | service_level | olap     | `CREATE SERVICE_LEVEL olap WITH SHARES = 100;` |

#### Notes

The options in the create statement are always in the following order: `TIMEOUT`, `WORKLOAD_TYPE`, `SHARES`.

### Attaching service levels to roles

```sql
CREATE ROLE nicholas WITH PASSWORD = "okoń" AND LOGIN = true;
CREATE SERVICE_LEVEL olap WITH SHARES = 100;
ATTACH SERVICE_LEVEL olap TO nicholas;
```

| `keyspace_name` | `type`                     |  `name`  | `create_statement` |
| :-------------: | :------------------------: | :------: | :----------------: |
| ""              | service_level_attachment   | olap     | `ATTACH SERVICE_LEVEL olap TO nicholas;` |

[^1]: See: `docs/operating-scylla/procedures/backup-restore`.
