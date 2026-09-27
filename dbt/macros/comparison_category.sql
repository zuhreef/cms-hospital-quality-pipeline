{#- Normalise CMS's many "compared to national" phrasings into 4 buckets.
    Covers rate-based ("Better Than the National Rate"), value-based and the
    excess-days measures ("Fewer Days Than Average per 100 Discharges"). -#}
{% macro comparison_category(col) -%}
    case
        when lower({{ col }}) like 'better%' or lower({{ col }}) like 'fewer%' then 'better'
        when lower({{ col }}) like 'worse%'  or lower({{ col }}) like 'more%'  then 'worse'
        when lower({{ col }}) like 'no different%' or lower({{ col }}) like 'average%' then 'no_different'
        when lower({{ col }}) like '%too small%' then 'too_few_cases'
        else 'not_available'
    end
{%- endmacro %}
