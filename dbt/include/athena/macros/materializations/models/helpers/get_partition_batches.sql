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
    {%- set partitions_batches = [] -%}
    {%- set ns = namespace(bucket_values={}, bucket_column=None, single_partition=[]) -%}

    {%- for row in rows -%}
        {%- set ns.single_partition = [] -%}
        {%- for col, partition_key in zip(row, partitioned_by) -%}
            {%- set column_type = adapter.convert_type(table, loop.index0) -%}
            {%- set comp_func = '=' -%}
            {%- set bucket_match = modules.re.search('bucket\((.+),.+([0-9]+)\)', partition_key) -%}
            {%- if bucket_match -%}
                {# Handle bucketed partition #}
                {%- set ns.bucket_column = bucket_match[1] -%}
                {%- set bucket_num = adapter.murmur3_hash(col, bucket_match[2] | int) -%}
                {%- if bucket_num not in ns.bucket_values %}
                    {%- do ns.bucket_values.update({bucket_num: set()}) %}
                {%- endif %}
                {# Format value based on column type #}
                {%- if column_type == 'string' -%}
                    {%- set formatted_value = "'" + col | string + "'" -%}
                {%- elif column_type == 'integer' -%}
                    {%- set formatted_value = col | string -%}
                {%- elif column_type == 'date' -%}
                    {%- set formatted_value = "DATE'" + col | string + "'" -%}
                {%- elif column_type == 'timestamp' -%}
                    {%- set formatted_value = "TIMESTAMP'" + col | string + "'" -%}
                {%- else -%}
                    {%- do exceptions.raise_compiler_error('Need to add support for column type ' + column_type) -%}
                {%- endif -%}
                {%- do ns.bucket_values[bucket_num].add(formatted_value) -%}
            {%- else -%}
                {# Existing logic for non-bucketed columns #}
                {%- if col is none -%}
                    {%- set value = 'null' -%}
                    {%- set comp_func = ' is ' -%}
                {%- elif column_type == 'integer' or column_type is none -%}
                    {%- set value = col | string -%}
                {%- elif column_type == 'string' -%}
                    {%- set value = "'" + col + "'" -%}
                {%- elif column_type == 'date' -%}
                    {%- set value = "DATE'" + col | string + "'" -%}
                {%- elif column_type == 'timestamp' -%}
                    {%- set value = "TIMESTAMP'" + col | string + "'" -%}
                {%- else -%}
                    {%- do exceptions.raise_compiler_error('Need to add support for column type ' + column_type) -%}
                {%- endif -%}
                {%- set partition_key = adapter.format_one_partition_key(partitioned_by[loop.index0]) -%}
                {%- do ns.single_partition.append(partition_key + comp_func + value) -%}
            {%- endif -%}
        {%- endfor -%}

        {%- for bucket_num, values in ns.bucket_values.items() -%}
            {%- do ns.single_partition.append(ns.bucket_column + " IN (" + values | list | join(", ") + ")") -%}
        {%- endfor -%}

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
