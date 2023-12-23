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
    {% do log('TOTAL PARTITIONS TO PROCESS: ' ~ rows | length) %}
    {%- set partitions_batches = [] -%}
    {%- set bucket_map = {} -%}  {# Map for each bucket number with its column and values #}

    {%- for row in rows -%}
        {%- set single_partition = [] -%}

        {%- for col, partition_key in zip(row, partitioned_by) -%}
            {%- set column_type = adapter.convert_type(table, loop.index0) -%}
            {%- set bucket_match = modules.re.search('bucket\((.+),.+([0-9]+)\)', partition_key) -%}
            {%- if bucket_match -%}
                {%- set bucket_column = bucket_match[1] -%}
                {%- set bucket_num = adapter.murmur3_hash(col, bucket_match[2] | int) -%}
                {%- set formatted_value = adapter.format_value_for_partition(col, column_type) -%}

                {# Manage bucket values and column mapping #}
                {% if bucket_num not in bucket_map %}
                    {% do bucket_map.update({bucket_num: {'column': bucket_column, 'values': [formatted_value]}}) %}
                {% elif formatted_value not in bucket_map[bucket_num]['values'] %}
                    {% do bucket_map[bucket_num]['values'].append(formatted_value) %}
                {% endif %}
            {%- else -%}
                {# Non-bucketed columns #}
                {%- set value = adapter.format_value_for_partition(col, column_type) -%}
                {%- set partition_key_formatted = adapter.format_one_partition_key(partition_key) -%}
                {%- do single_partition.append(partition_key_formatted + " = " + value) -%}
            {%- endif -%}
        {%- endfor -%}

        {# Create IN clauses for each bucket #}
        {%- for bucket_info in bucket_map.values() -%}
            {%- set bucket_column = bucket_info['column'] %}
            {%- set unique_values = bucket_info['values'] | unique | join(", ") -%}
            {%- do single_partition.append(bucket_column + " IN (" + unique_values + ")") -%}
        {%- endfor -%}

        {# Combine all conditions for the current partition #}
        {%- set single_partition_expression = single_partition | join(' and ') -%}
        {%- set batch_number = (loop.index0 / athena_partitions_limit) | int -%}
        {% if batch_number not in partitions_batches %}
            {% do partitions_batches.append([]) %}
        {% endif %}
        {% do partitions_batches[batch_number].append('(' + single_partition_expression + ')') %}
        {%- if partitions_batches[batch_number] | length == athena_partitions_limit or loop.last -%}
            {%- do partitions_batches.append(partitions_batches[batch_number] | join(' or ')) -%}
        {%- endif -%}
    {%- endfor -%}

    {{ return(partitions_batches) }}
{%- endmacro %}
