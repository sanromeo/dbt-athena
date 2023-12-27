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
    {%- set ns = namespace(partitions = [], bucket_conditions = {}, bucket_numbers = [], bucket_column = None, is_bucketed = false) -%}

    {%- for row in rows -%}
        {%- set single_partition = [] -%}
        {%- for col, partition_key in zip(row, partitioned_by) -%}
            {%- set column_type = adapter.convert_type(table, loop.index0) -%}
            {%- set bucket_match = modules.re.search('bucket\((.+),.+([0-9]+)\)', partition_key) -%}
            {%- if bucket_match -%}
                {%- set ns.is_bucketed = true -%}
                {%- set ns.bucket_column = bucket_match[1] -%}
                {%- set bucket_num = adapter.murmur3_hash(col, bucket_match[2] | int) -%}
                {%- set formatted_value, comp_func = adapter.format_value_for_partition(col, column_type) -%}
                {%- if bucket_num not in ns.bucket_numbers %}
                    {%- do ns.bucket_numbers.append(bucket_num) %}
                    {%- do ns.bucket_conditions.update({bucket_num: [formatted_value]}) -%}
                {%- elif formatted_value not in ns.bucket_conditions[bucket_num] %}
                    {%- do ns.bucket_conditions[bucket_num].append(formatted_value) -%}
                {%- endif -%}
            {%- else -%}
                {%- set value, comp_func = adapter.format_value_for_partition(col, column_type) -%}
                {%- set partition_key_formatted = adapter.format_one_partition_key(partitioned_by[loop.index0]) -%}
                {%- do single_partition.append(partition_key_formatted + comp_func + value) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- set single_partition_expression = single_partition | join(' and ') -%}
        {%- if single_partition_expression not in ns.partitions %}
            {%- do ns.partitions.append(single_partition_expression) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- if ns.is_bucketed -%}
        {%- set total_batches = ns.partitions | length * ns.bucket_numbers | length -%}
    {%- else -%}
        {%- set total_batches = ns.partitions | length -%}
    {%- endif -%}
    {%- set batches_per_partition_limit = (total_batches // athena_partitions_limit) + (total_batches % athena_partitions_limit > 0) -%}

    {# Log the total number of partitions #}
    {% do log('TOTAL PARTITIONS TO PROCESS: ' ~ total_batches) %}

    {%- set partitions_batches = [] -%}
    {%- for i in range(batches_per_partition_limit) -%}
        {%- set batch_conditions = [] -%}
        {%- if ns.is_bucketed -%}
            {%- for partition_expression in ns.partitions -%}
                {%- for bucket_num in ns.bucket_numbers -%}
                    {%- set bucket_condition = ns.bucket_column + " IN (" + ns.bucket_conditions[bucket_num] | join(", ") + ")" -%}
                    {%- set combined_condition = "(" + partition_expression + ' and ' + bucket_condition + ")" -%}
                    {%- do batch_conditions.append(combined_condition) -%}
                {%- endfor -%}
            {%- endfor -%}
        {%- else -%}
            {%- do batch_conditions.extend(ns.partitions) -%}
        {%- endif -%}
        {%- set start_index = i * athena_partitions_limit -%}
        {%- set end_index = start_index + athena_partitions_limit -%}
        {%- do partitions_batches.append(batch_conditions[start_index:end_index] | join(' or ')) -%}
    {%- endfor -%}

    {{ return(partitions_batches) }}

{%- endmacro %}
