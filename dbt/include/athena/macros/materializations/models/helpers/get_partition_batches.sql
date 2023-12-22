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
    {%- set partitions = {} -%}
    {%- set partitions_batches = [] -%}
    {%- set bucket_data = [] -%}  {# List of tuples for bucket data #}
    {%- set ns = namespace(single_partition=[]) -%}

    {%- for row in rows -%}
        {%- set ns.single_partition = [] -%}
        {%- for col, partition_key in zip(row, partitioned_by) -%}
            {%- set column_type = adapter.convert_type(table, loop.index0) -%}
            {%- set bucket_match = modules.re.search('bucket\((.+),.+([0-9]+)\)', partition_key) -%}
            {%- if bucket_match -%}
                {%- set bucket_column = bucket_match[1] -%}
                {%- set bucket_num = adapter.murmur3_hash(col, bucket_match[2] | int) -%}
                {%- set formatted_value = adapter.format_value_for_partition(col, column_type) -%}
                {# Update bucket data with new value #}
                {% do bucket_data.append((bucket_num, bucket_column, formatted_value)) %}
            {%- else -%}
                {# Handle non-bucketed columns #}
                {%- set value = adapter.format_value_for_partition(col, column_type) -%}
                {%- set partition_key = adapter.format_one_partition_key(partitioned_by[loop.index0]) -%}
                {%- do ns.single_partition.append(partition_key + " = " + value) -%}
            {%- endif -%}
        {%- endfor -%}

        {# Process bucket data and add to single_partition #}
        {%- for bucket_num, bucket_column, formatted_value in bucket_data -%}
            {%- set bucket_values = bucket_data | selectattr('0', 'equalto', bucket_num) | map('last') | unique | list -%}
            {%- do ns.single_partition.append(bucket_column + " IN (" + bucket_values | join(", ") + ")") -%}
        {%- endfor -%}

        {# Combine all conditions for the current partition #}
        {%- set single_partition_expression = ns.single_partition | join(' and ') -%}
        {%- set batch_number = (loop.index0 / athena_partitions_limit) | int -%}
        {% if not batch_number in partitions %}
            {% do partitions.update({batch_number: []}) %}
        {% endif %}
        {%- do partitions[batch_number].append('(' + single_partition_expression + ')') -%}
        {%- if partitions[batch_number] | length == athena_partitions_limit or loop.last -%}
            {%- do partitions_batches.append(partitions[batch_number] | join(' or ')) -%}
        {%- endif -%}
    {%- endfor -%}

    {{ return(partitions_batches) }}
{%- endmacro %}
