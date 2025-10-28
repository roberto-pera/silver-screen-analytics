-- analyses/generate_sources.sql

{{ codegen.generate_source(
    database_name='SILVERSCREEN',
    schema_name='PUBLIC',
    include_descriptions=True
) }}
