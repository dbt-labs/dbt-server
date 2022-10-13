FILES = {}

FILES[
    "dbt_project.yml"
] = """
name: 'my_second_project'
version: '1.0.0'
config-version: 2

profile: 'user'

source-paths: ["models"]
analysis-paths: ["analysis"]
test-paths: ["tests"]
seed-paths: ["data"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

target-path: "target"  # directory which will store compiled SQL FILES
clean-targets:         # directories to be removed by `dbt clean`
  - "target"
  - "dbt_modules"
"""

FILES[
    "models/model_1.sql"
] = """
select * from {{ ref('model_2') }}
"""

FILES[
    "models/model_2.sql"
] = """
select {{ 1 + 1 }}
"""


FILES[
    "macros/my_macro.sql"
] = """
{% macro my_macro(name) -%}
hello, {{ name }}!
{%- endmacro %}
"""
