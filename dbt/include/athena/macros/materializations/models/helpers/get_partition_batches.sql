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
    {%- set bucket_conditions = {} -%}  {# Map for storing unique bucket conditions #}

    {%- for row in rows -%}
        {%- set single_partition = [] -%}

        {# Generate partition conditions #}
        {%- for col, partition_key in zip(row, partitioned_by) -%}
            {%- if 'bucket(' not in partition_key -%}
                {%- set column_type = adapter.convert_type(table, loop.index0) -%}
                {%- set value = adapter.format_value_for_partition(col, column_type) -%}
                {%- set partition_key_formatted = adapter.format_one_partition_key(partition_key) -%}
                {%- do single_partition.append(partition_key_formatted + " = " + value) -%}
            {%- endif -%}
        {%- endfor -%}

        {# Generate and store unique bucket conditions #}
        {%- for col, partition_key in zip(row, partitioned_by) -%}
            {%- if 'bucket(' in partition_key -%}
                {%- set bucket_match = modules.re.search('bucket\((.+),.+([0-9]+)\)', partition_key) -%}
                {%- set bucket_column = bucket_match[1] -%}
                {%- set bucket_num = adapter.murmur3_hash(col, bucket_match[2] | int) -%}
                {%- set formatted_value = adapter.format_value_for_partition(col, adapter.convert_type(table, loop.index0)) -%}
                {# Add value to bucket condition map #}
                {% if bucket_num not in bucket_conditions %}
                    {% do bucket_conditions.update({bucket_num: {'column': bucket_column, 'values': [formatted_value]}}) %}
                {% elif formatted_value not in bucket_conditions[bucket_num]['values'] %}
                    {% do bucket_conditions[bucket_num]['values'].append(formatted_value) %}
                {% endif %}
            {%- endif -%}
        {%- endfor -%}

        {# Combine partition and bucket conditions #}
        {%- for bucket_info in bucket_conditions.values() -%}
            {%- set bucket_column = bucket_info['column'] %}
            {%- set unique_values = bucket_info['values'] | unique | join(", ") -%}
            {%- do single_partition.append(bucket_column + " IN (" + unique_values + ")") -%}
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
    {%- for batch in partitions_batches -%}
        {%- do partitions_batches.append(batch | join(' or ')) -%}
    {%- endfor -%}

    {{ return(partitions_batches) }}
{%- endmacro %}
