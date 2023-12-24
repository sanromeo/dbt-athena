{% macro get_partition_batches(sql, as_subquery=True) -%}
    {%- set partitioned_by = config.get('partitioned_by') -%}
    {%- set athena_partitions_limit = config.get('partitions_limit', 100) | int -%}
    {%- set partitioned_keys = adapter.format_partition_keys(partitioned_by) -%}
    {% do log('PARTITIONED KEYS: ' ~ partitioned_keys) %}

    {% call statement('get_partitions', fetch_result=True) %}
        {%- if as_subquery -%}
            select distinct {{ partitioned_keys }} from ({{ sql }}) order by {{ partitioned_keys }};
        {%- else -%}
            select distinct {{ partitioned_keys }} from {{ sql }} order by {{ partitioned_keys }};
        {%- endif -%}
    {% endcall %}

    {%- set table = load_result('get_partitions').table -%}
    {%- set rows = table.rows -%}
    {%- set partitions = {} -%}
    {% do log('TOTAL PARTITIONS TO PROCESS: ' ~ rows | length) %}
    {%- set bucket_map = {} -%}

    {%- for row in rows -%}
        {%- set single_partition = [] -%}
        {%- set bucket_conditions = [] -%}
        {%- for col, partition_key in zip(row, partitioned_by) -%}
            {%- set column_type = adapter.convert_type(table, loop.index0) -%}
            {%- if 'bucket(' in partition_key -%}
                {%- set bucket_match = modules.re.search('bucket\((.+),.+([0-9]+)\)', partition_key) -%}
                {%- set bucket_column = bucket_match[1] -%}
                {%- set bucket_num = adapter.murmur3_hash(col, bucket_match[2] | int) -%}
                {%- set formatted_value = adapter.format_value_for_partition(col, column_type) -%}
                {% if bucket_num not in bucket_map %}
                    {% do bucket_map.update({bucket_num: {'column': bucket_column, 'values': [formatted_value]}}) %}
                {% elif formatted_value not in bucket_map[bucket_num]['values'] %}
                    {% do bucket_map[bucket_num]['values'].append(formatted_value) %}
                {% endif %}
            {%- else -%}
                {%- set value = adapter.format_value_for_partition(col, column_type) -%}
                {%- set partition_key_formatted = adapter.format_one_partition_key(partition_key) -%}
                {%- do single_partition.append(partition_key_formatted + " = " + value) -%}
            {%- endif -%}
        {%- endfor -%}

        {# Process bucket conditions for the current row #}
        {%- for bucket_num, bucket_info in bucket_map.items() -%}
            {%- set unique_values = bucket_info['values'] | unique | join(", ") -%}
            {%- do bucket_conditions.append(bucket_info['column'] + " IN (" + unique_values + ")") -%}
        {%- endfor -%}

        {# Combine partition conditions with bucket conditions #}
        {%- set combined_conditions = single_partition + bucket_conditions -%}
        {%- set combined_expression = combined_conditions | join(' and ') -%}
        {%- set batch_number = (loop.index0 / athena_partitions_limit) | int -%}
        {% if batch_number not in partitions %}
            {% do partitions.update({batch_number: []}) %}
        {% endif %}
        {% do partitions[batch_number].append('(' + combined_expression + ')') %}

    {%- endfor -%}

    {# Combine partitions into batches #}
    {%- set partitions_batches = [] -%}
    {%- for batch_partitions in partitions.values() -%}
        {%- do partitions_batches.append(batch_partitions | join(' or ')) -%}
    {%- endfor -%}

    {{ return(partitions_batches) }}
{%- endmacro %}
