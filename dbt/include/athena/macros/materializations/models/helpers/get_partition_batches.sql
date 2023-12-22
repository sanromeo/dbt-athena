{% macro get_partition_batches(sql, as_subquery=True) -%}
    {%- set ns_bc = namespace(bucket_column=None) -%}
    {%- set ns_sp = namespace(single_partition=[]) -%}
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

    {%- for row in rows -%}
        {%- set ns_sp.single_partition = [] -%}
        {%- set bucket_values = {} -%}
        {%- for col, partition_key in zip(row, partitioned_by) -%}
            {%- set column_type = adapter.convert_type(table, loop.index0) -%}
            {%- set comp_func = '=' -%}
            {%- set bucket_match = modules.re.search('bucket\((.+),.+([0-9]+)\)', partition_key) -%}
            {%- if bucket_match -%}
                {# Handle bucketed partition #}
                {%- set ns_bc.bucket_column = bucket_match[1] -%}
                {%- set bucket_num = adapter.murmur3_hash(col, bucket_match[2] | int) -%}
                {%- if bucket_num not in bucket_values %}
                    {%- do bucket_values.update({bucket_num: {'values': [], 'type': None}}) %}
                {%- endif %}
                {%- set bucket_column_type = adapter.convert_type(table, loop.index0) -%}
                {%- do bucket_values[bucket_num]['values'].append(col) -%}
                {%- do bucket_values[bucket_num].update({'type': bucket_column_type}) -%}
                {% do log('Adding value ' ~ col ~ ' to bucket ' ~ bucket_num) %}
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
                {%- do ns_sp.single_partition.append(partition_key + comp_func + value) -%}
            {%- endif -%}
        {%- endfor -%}

        {%- for bucket_num, bucket_info in bucket_values.items() -%}
            {%- set formatted_values = [] -%}
            {%- for value in bucket_info['values'] -%}
                {# Format each value based on its type #}
                {%- if bucket_info['type'] == 'string' -%}
                    {%- do formatted_values.append("'" + value | string + "'") -%}
                {%- elif bucket_info['type'] == 'integer' -%}
                    {%- do formatted_values.append(value | string) -%}
                {%- elif bucket_info['type'] == 'date' -%}
                    {%- do formatted_values.append("DATE'" + value | string + "'") -%}
                {%- elif bucket_info['type'] == 'timestamp' -%}
                    {%- do formatted_values.append("TIMESTAMP'" + value | string + "'") -%}
                {%- else -%}
                    {%- do exceptions.raise_compiler_error('Need to add support for column type ' + bucket_info['type']) -%}
                {%- endif -%}
            {%- endfor -%}
            {% do log('Bucket ' ~ bucket_num ~ ' values: ' ~ formatted_values) %}
            {%- do ns_sp.single_partition.append(ns_bc.bucket_column + " IN (" + formatted_values | join(", ") + ")") -%}
        {%- endfor -%}

        {%- set single_partition_expression = ns_sp.single_partition | join(' and ') -%}
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
