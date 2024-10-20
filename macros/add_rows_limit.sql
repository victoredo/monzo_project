{% macro add_rows_limit(number_rows=1000) %}

    {% if var("dev_limit") == true and target.name == 'default' %}

        limit {{ number_rows }}

    {% endif %}

{% endmacro %}