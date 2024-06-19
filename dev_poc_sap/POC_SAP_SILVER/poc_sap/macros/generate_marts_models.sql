{% macro generate_mart_models() %}
{% set staging_path = "models/staging" %}
{% set marts_path = "models/marts/SalesOrders" %}
{% set staging_models = dbt_utils.get_relations_by_prefix(schema=target.schema, prefix='') %}

{% for model in staging_models %}
  {% set model_name = model.identifier %}
  {% if model_name.endswith('MD') %}
    {% set mart_model_name = model_name + '_V' %}
    {% set model_path = marts_path + "/" + mart_model_name + ".sql" %}
    {% set sql_content %}
select *
from {{ ref('{{ model_name }}') }} as {{ model_name | lower }}
{% endset %}
    {{ dbt.write_file(path=model_path, contents=sql_content) }}
  {% endif %}
{% endfor %}
{% endmacro %}
