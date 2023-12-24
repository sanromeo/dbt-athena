{% macro get_partition_batches(sql, as_subquery=True) -%}
    {%- set partitioned_by = config.get('partitioned_by') -%}
    {%- set athena_partitions_limit = config.get('partitions_limit', 100) | int -%}
    {%- set partitioned_keys = adapter.format_partition_keys(partitioned_by) -%}
    {% do log('PARTITIONED KEYS: ' ~ partitioned_keys) %}

    {# Gather distinct values for partition and bucket keys #}
    {% call statement('get_partitions', fetch_result=True) %}
        {%- if as_subquery -%}
            select distinct {{ partitioned_keys }} from ({{ sql }}) order by {{ partitioned_keys }};
        {%- else -%}
            select distinct {{ partitioned_keys }} from {{ sql }} order by {{ partitioned_keys }};
        {%- endif -%}
    {% endcall %}

    {%- set table = load_result('get_partitions').table -%}
    {%- set rows = table.rows -%}
    {%- set partitions_batches = [] -%}
    {%- set bucket_values = {} -%}

    {%- for row in rows -%}
        {%- set single_partition = [] -%}
        {%- for col, partition_key in zip(row, partitioned_by) -%}
            {%- set column_type = adapter.convert_type(table, loop.index0) -%}
            {%- if 'bucket(' in partition_key -%}
                {# Process bucket columns #}
                {%- set bucket_match = modules.re.search('bucket\((.+),.+([0-9]+)\)', partition_key) -%}
                {%- set bucket_column = bucket_match[1] -%}
                {%- set bucket_num = adapter.murmur3_hash(col, bucket_match[2] | int) -%}
                {%- set formatted_value = adapter.format_value_for_partition(col, column_type) -%}
                {% if bucket_num not in bucket_values %}
                    {% do bucket_values.update({bucket_num: {'column': bucket_column, 'values': [formatted_value]}}) %}
                {% else %}
                    {% do bucket_values[bucket_num]['values'].append(formatted_value) %}
                {% endif %}
            {%- else -%}
                {# Process non-bucket columns #}
                {%- set value = adapter.format_value_for_partition(col, column_type) -%}
                {%- set partition_key_formatted = adapter.format_one_partition_key(partition_key) -%}
                {%- do single_partition.append(partition_key_formatted + " = " + value) -%}
            {%- endif -%}
        {%- endfor -%}

        {# Add bucket conditions to single partition #}
        {%- for bucket_info in bucket_values.values() -%}
            {%- set unique_values = bucket_info['values'] | unique | join(", ") -%}
            {%- do single_partition.append(bucket_info['column'] + " IN (" + unique_values + ")") -%}
        {%- endfor -%}

        {# Finalize the condition for the current partition #}
        {%- set single_partition_expression = single_partition | join(' and ') -%}
        {%- set batch_number = (loop.index0 / athena_partitions_limit) | int -%}
        {% if batch_number not in partitions_batches %}
            {% do partitions_batches.append([]) %}
        {% endif %}
        {% do partitions_batches[batch_number].append('(' + single_partition_expression + ')') %}
    {%- endfor -%}

    {# Combine all batches into final query parts #}
    {%- set final_query_parts = [] -%}
    {%- for batch in partitions_batches -%}
        {%- do final_query_parts.append(batch | join(' or ')) -%}
    {%- endfor -%}

    {{ return(final_query_parts) }}
{%- endmacro %}
