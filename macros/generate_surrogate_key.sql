{% macro generate_surrogate_key(*args) %}
    md5(concat({{ ', '.join(args) }}))
{% endmacro %}