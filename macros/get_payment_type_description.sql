{#
    This macro retur the description of the payment_type
#}

{% macro get_payment_type_description(payment_type) -%}

    CASE {{payment_type}}
        WHEN 0 THEN 'N/A'
        WHEN 1 THEN 'Credit Card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No Charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided Trip'
    END

{%- endmacro %}