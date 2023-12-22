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
    {%- set ns = namespace(bucket_map={}, single_partition=[], bucket_column=None) -%}

    {%- for row in rows -%}
        {%- set ns.single_partition = [] -%}
        {%- set ns.bucket_map = {} -%}
        {%- for col, partition_key in zip(row, partitioned_by) -%}
            {%- set column_type = adapter.convert_type(table, loop.index0) -%}
            {%- set bucket_match = modules.re.search('bucket\((.+),.+([0-9]+)\)', partition_key) -%}
            {%- if bucket_match -%}
                {%- set ns.bucket_column = bucket_match[1] -%}
                {%- set bucket_num = adapter.murmur3_hash(col, bucket_match[2] | int) -%}
                {%- set formatted_value = adapter.format_value_for_partition(col, column_type) -%}
                {# Aggregate values by bucket number #}
                {% do ns.bucket_map.setdefault(bucket_num, []).append(formatted_value) %}
            {%- else -%}
                {%- set value = adapter.format_value_for_partition(col, column_type) -%}
                {%- set partition_key_formatted = adapter.format_one_partition_key(partitioned_by[loop.index0]) -%}
                {%- do ns.single_partition.append(partition_key_formatted + " = " + value) -%}
            {%- endif -%}
        {%- endfor -%}

        {# Grouping values by bucket number #}
        {%- for bucket_num, values in ns.bucket_map.items() -%}
            {%- set unique_values = values | unique -%}
            {%- do ns.single_partition.append(ns.bucket_column + " IN (" + unique_values | join(", ") + ")") -%}
        {%- endfor -%}

        {%- set single_partition_expression = ns.single_partition | join(' and ') -%}
        {%- if single_partition_expression -%}
            {%- do partitions_batches.append('(' + single_partition_expression + ')') -%}
        {%- endif -%}
    {%- endfor -%}

    {%- if partitions_batches -%}
        {{ return(partitions_batches) }}
    {%- else -%}
        {{ return([]) }}
    {%- endif -%}
{%- endmacro %}
