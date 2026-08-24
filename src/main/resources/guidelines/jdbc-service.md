# JDBC service fragments

A JDBC service is split into two editable fragments:

- `jdbc-service.xml`: service configuration only
- `query.sql`: the SQL statement

## jdbc-service.xml

Supported elements:

- `connectionId`: id of the JDBC pool artifact to use, it should resolve to a jdbcPool artifact
- `changeTrackerId`: optional id of a service implementing `be.nabu.libs.services.jdbc.api.ChangeTracker.track`
- `inputDefinition`: optional explicit input type id. If not provided, the input is generated automatically
- `outputDefinition`: optional explicit output type id. If not provided, the output is generated automatically
- `generatedColumn`: optional generated key column name
- `validateInput`: optional boolean
- `validateOutput`: optional boolean

Example:

```xml
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<jdbcService>
	<connectionId>my.module.jdbc.pool</connectionId>
	<changeTrackerId>my.module.services.auditChangeTracker</changeTrackerId>
	<inputDefinition>my.module.types.MyInput</inputDefinition>
	<outputDefinition>my.module.types.MyOutput</outputDefinition>
	<generatedColumn>id</generatedColumn>
	<validateInput>true</validateInput>
	<validateOutput>false</validateOutput>
</jdbcService>
```

## Connection

In almost all cases you should NOT configure a specific connection id.
The system automatically chooses the correct connection depending on the call context.

## Selection

When you are selecting a customized resultset, you can either use automatically generated output or a custom structure. For automatically generated output, list every field and use "as" to provide a clean name for the field, for example `select a + b as sum`, the field will then be generated as `sum`. Note that fields are map by their order, NOT by name. Do NOT use `select *` or `select t.*` when you are using generated output.

When you are using a database structure as the output, it is better to use `*` e.g. `select * from tasks` with `nabu.frameworks.tasks.types.model.tasks.Task` as output.
When you are using a structure that is NOT annotated according to database rules, it is better to explicitly list all the fields you want to select in the correct order.

Nabu will automatically expand `*` to select the correct fields in the correct order and the SQL won't break if new fields are added. This also works with extensions though you have to make sure the necessary tables are joined in the `from`.
Nabu will NOT expand things like `t.*`, these are not supported.

## Prefixes

We generally add "~" before a table name which allows us to dynamically inject prefixes, for example:

```
select * from ~tasks
```

If there is no prefix at runtime, it just becomes `tasks` but if there is a prefix, it might become `pr_tasks`.

## Generated versus explicit input/output

If `inputDefinition` is omitted, the JDBC service generates its own input structure from `query.sql`.
If `outputDefinition` is omitted, the JDBC service generates its own output structure from `query.sql`.
When using generated inputs or outputs, you must **FIRST** set the SQL. The input/output will be generated based on that SQL and you can only modify them once they have been created.
Only set `inputDefinition` or `outputDefinition` when you want to override the generated structure fields with explicit scalar types.

IF the input is generated, the input structure is available under the fragment `parameters.xml` .
IF the output is generated, the output structure is available under the fragment `results.xml`.
If they are not generated, the fragments do not exist and should not be written to.
If they do exist, you MUST update them to match the correct types. You should never add or remove fields from these two fragments.
All parameter and result fields start as a string but you MUST update them to be the exact correct type, for example UUID, long,...

## query.sql

`query.sql` contains the raw SQL only. Example:

```sql
select
	id,
	name
from customers
where status = :status
```

## limit/offset

Limit and offset should NOT be hardcoded into the sql because we want to keep the flexibility to change it.
Use the limit/offset in the input of the service instead.