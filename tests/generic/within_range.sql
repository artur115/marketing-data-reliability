{#- Fails for every row where the column falls outside [min_value, max_value].
    Small on purpose: no package dependency needed. -#}
{% test within_range(model, column_name, min_value, max_value) %}
select {{ column_name }} as out_of_range_value
from {{ model }}
where {{ column_name }} < {{ min_value }}
   or {{ column_name }} > {{ max_value }}
{% endtest %}
