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
    {%- set unique_partition_expressions = [] -%}
    {%- set bucket_counts = {} -%}
    {%- set combined_conditions = [] -%}

    {%- for row in rows -%}
        {%- set partition_conditions = [] -%}
        {%- set bucket_conditions = {} -%}

        {%- for col, partition_key in zip(row, partitioned_by) -%}
            {%- set column_type = adapter.convert_type(table, loop.index0) -%}
            {%- set bucket_match = modules.re.search('bucket\((.+),.+([0-9]+)\)', partition_key) -%}

            {%- if bucket_match -%}
                {%- set bucket_column = bucket_match[1] -%}
                {%- set bucket_num = adapter.murmur3_hash(col, bucket_match[2] | int) -%}
                {%- set formatted_value = adapter.format_value_for_partition(col, column_type) -%}

                {%- if bucket_num not in bucket_conditions %}
                    {%- set bucket_conditions[bucket_num] = [formatted_value] -%}
                {%- else %}
                    {%- do bucket_conditions[bucket_num].append(formatted_value) %}
                {%- endif %}
            {%- else -%}
                {%- set value = adapter.format_value_for_partition(col, column_type) -%}
                {%- set partition_key_formatted = adapter.format_one_partition_key(partitioned_by[loop.index0]) -%}
                {%- do partition_conditions.append(partition_key_formatted + " = " + value) -%}
            {%- endif -%}
        {%- endfor -%}

        {%- set partition_expression = partition_conditions | join(' and ') -%}
        {%- if partition_expression not in unique_partition_expressions -%}
            {%- do unique_partition_expressions.append(partition_expression) -%}
        {%- endif -%}

        {%- for bucket_num, values in bucket_conditions.items() -%}
            {%- set bucket_condition = bucket_column + " IN (" + values | unique | join(", ") + ")" -%}
            {%- do combined_conditions.append(partition_expression + ' and ' + bucket_condition) -%}
            {%- set count = bucket_counts.get(bucket_num, 0) + 1 -%}
            {%- do bucket_counts.update({bucket_num: count}) -%}
        {%- endfor -%}

        {%- if not bucket_conditions -%}
            {%- do combined_conditions.append(partition_expression) -%}
        {%- endif -%}
    {%- endfor -%}

    {%- set total_batches = (unique_partition_expressions | length) * max(bucket_counts.values(), default=1) -%}
    {%- set partitions_batches = [] -%}

    {%- for i in range((total_batches / athena_partitions_limit) | ceil) -%}
        {%- set batch_conditions = combined_conditions[i * athena_partitions_limit : (i + 1) * athena_partitions_limit] -%}
        {%- do partitions_batches.append(batch_conditions | join(' or ')) -%}
    {%- endfor -%}

    {{ return(partitions_batches) }}
{%- endmacro %}
