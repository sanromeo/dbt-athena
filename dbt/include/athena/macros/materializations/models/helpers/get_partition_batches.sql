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
    {%- set bucket_partitions = {} -%}
    {%- set non_bucket_partitions = [] -%}

    {# Process each row to group values by bucket number and handle non-bucketed partitions #}
    {%- for row in rows -%}
        {%- set single_partition = [] -%}
        {%- for col, partition_key in zip(row, partitioned_by) -%}
            {%- set column_type = adapter.convert_type(table, loop.index0) -%}
            {%- set bucket_match = modules.re.search('bucket\((.+),\s*(\d+)\)', partition_key) -%}
            {%- if bucket_match -%}
                {# Handle bucketed partition #}
                {%- set bucket_column = bucket_match[1] -%}
                {%- set num_buckets = bucket_match[2] | int -%}
                {%- set bucket_num = adapter.murmur3_hash(col, num_buckets) -%}
                {%- if bucket_num not in bucket_partitions -%}
                    {%- do bucket_partitions.update({bucket_num: []}) -%}
                {%- endif -%}
                {%- do bucket_partitions[bucket_num].append(col) -%}
            {%- else -%}
                {# Handle non-bucketed partitions #}
                {%- if col is none -%}
                    {%- set formatted_value = 'null' -%}
                {%- elif column_type == 'integer' or column_type == 'long' -%}
                    {%- set formatted_value = col | string -%}
                {%- elif column_type == 'string' -%}
                    {%- set formatted_value = "'" + col + "'" -%}
                {%- elif column_type == 'date' -%}
                    {%- set formatted_value = "DATE'" + col | string + "'" -%}
                {%- elif column_type == 'timestamp' -%}
                    {%- set formatted_value = "TIMESTAMP'" + col | string + "'" -%}
                {%- else -%}
                    {%- do exceptions.raise_compiler_error('Need to add support for column type ' + column_type) -%}
                {%- endif -%}
                {%- do single_partition.append(formatted_value) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- if single_partition|length > 0 -%}
            {%- do non_bucket_partitions.append('(' + single_partition | join(' and ') + ')') -%}
        {%- endif -%}
    {%- endfor -%}

    {# Create batches combining bucket values and non-bucket partitions #}
    {%- set partitions_batches = [] -%}
    {%- for bucket_num, values in bucket_partitions.items() -%}
        {%- set bucket_conditions = [] -%}
        {%- for value in values -%}
            {# Create WHERE clause for each value in this bucket #}
            {%- if column_type == 'string' -%}
                {%- set formatted_value = "'" + value | string + "'" -%}
            {%- elif column_type == 'date' -%}
                {%- set formatted_value = "DATE'" + value | string + "'" -%}
            {%- elif column_type == 'timestamp' -%}
                {%- set formatted_value = "TIMESTAMP'" + value | string + "'" -%}
            {%- else -%}
                {%- set formatted_value = value | string -%}
            {%- endif -%}
            {%- do bucket_conditions.append(bucket_column + " = " + formatted_value) -%}
        {%- endfor -%}
        {# Combine conditions for this bucket #}
        {%- set bucket_condition = bucket_conditions | join(' or ') -%}
        {# Add non-bucket conditions #}
        {%- for non_bucket in non_bucket_partitions -%}
            {%- do bucket_condition = bucket_condition + ' and ' + non_bucket -%}
        {%- endfor -%}
        {%- do partitions_batches.append(bucket_condition) -%}
    {%- endfor -%}

    {{ return(partitions_batches) }}

{%- endmacro %}
