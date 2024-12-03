{% macro generate_hash_diff(*args) %}
    md5(concat({{ ', '.join(args) }}))
{% endmacro %}