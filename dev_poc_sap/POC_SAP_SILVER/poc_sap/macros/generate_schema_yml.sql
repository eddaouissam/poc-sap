{% macro generate_schema_yml(schema_name) %}
{% set results = run_query("SELECT table_name FROM information_schema.tables WHERE table_schema = '" ~ schema_name ~ "'") %}

{% if execute %}
  {% set table_names = [] %}
  {% for row in results %}
    {% do table_names.append(row['TABLE_NAME']) %}
  {% endfor %}
  
  {% set yaml_output = "version: 2\nmodels:\n" %}
  {% for table_name in table_names %}
    {% set yaml_output = yaml_output + "  - name: " + table_name + "\n    description: \"Description for " + table_name + "\"\n    columns:\n      - name: id\n        description: \"Primary key\"\n" %}
  {% endfor %}
  
  {{ yaml_output }}
{% endif %}
{% endmacro %}
