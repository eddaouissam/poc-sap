{% macro generate_source_yml(database_name, schema_names) %}
{% set results = [] %}

-- Start debug output
{% do log("Starting to generate source.yml", info=True) %}

{% for schema in schema_names %}
    {% do log("Processing schema: " ~ schema, info=True) %}
    {% set query = "SELECT table_name FROM " ~ database_name ~ ".information_schema.tables WHERE table_schema = '" ~ schema ~ "'" %}
    {% do log("Running query: " ~ query, info=True) %}
    {% set tables_in_schema = run_query(query) %}
    
    {% if execute %}
        {% do log("Query executed", info=True) %}
        {% set table_list = [] %}
        {% for row in tables_in_schema %}
            {% set table_list = table_list + [row['table_name']] %}
        {% endfor %}
        
        {% if table_list | length > 0 %}
            {% set source_yml %}
version: 2

sources:
  - name: {{ schema }}
    database: {{ database_name }}
    schema: {{ schema }}
    tables:
      {% for table in table_list %}
      - name: {{ table }}
      {% endfor %}
{% endset %}
            {% do log("Generated source_yml for schema " ~ schema ~ ": " ~ source_yml, info=True) %}
            {% do results.append(source_yml.strip()) %}
        {% else %}
            {% do log("No tables found for schema: " ~ schema, info=True) %}
        {% endif %}
    {% else %}
        {% do log("Query did not execute", info=True) %}
    {% endif %}
{% endfor %}

-- End debug output
{% do log("Completed generating source.yml", info=True) %}

{{ results | join('\n\n') }}

{% endmacro %}
