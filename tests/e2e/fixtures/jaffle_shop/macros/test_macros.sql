-- Testing purpose
{% macro test_macro(int_value=2) %}
    {{ int_value }} + {{var("test_var")}}
{% endmacro %}
