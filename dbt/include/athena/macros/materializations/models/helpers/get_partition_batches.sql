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
    {%- set bucket_conditions = {} -%}

    {%- for row in rows -%}
        {%- set single_partition = [] -%}

        {%- for col, partition_key in zip(row, partitioned_by) -%}
            {%- set column_type = adapter.convert_type(table, loop.index0) -%}
            {%- set bucket_match = modules.re.search('bucket\((.+),.+([0-9]+)\)', partition_key) -%}

            {%- if bucket_match -%}
                {%- set bucket_column = bucket_match[1] -%}
                {%- set bucket_num = adapter.murmur3_hash(col, bucket_match[2] | int) -%}
                {%- set formatted_value = adapter.format_value_for_partition(col, column_type) -%}

                {% if bucket_num not in bucket_conditions %}
                    {% do bucket_conditions.update({bucket_num: [formatted_value]}) %}
                {% else %}
                    {% do bucket_conditions[bucket_num].append(formatted_value) %}
                {% endif %}
            {%- else -%}
                {%- set value = adapter.format_value_for_partition(col, column_type) -%}
                {%- set partition_key_formatted = adapter.format_one_partition_key(partitioned_by[loop.index0]) -%}
                {%- do single_partition.append(partition_key_formatted + " = " + value) -%}
            {%- endif -%}
        {%- endfor -%}

        {%- for bucket_num, values in bucket_conditions.items() -%}
            {%- set bucket_condition = bucket_column + " IN (" + values | unique | join(", ") + ")" -%}
            {%- do single_partition.append(bucket_condition) -%}
        {%- endfor -%}

        {%- set single_partition_expression = single_partition | join(' and ') -%}
        {%- set batch_number = (loop.index0 / athena_partitions_limit) | int -%}
        {%- if batch_number not in partitions_batches %}
            {% do partitions_batches.append([]) %}
        {% endif %}
        {%- do partitions_batches[batch_number].append('(' + single_partition_expression + ')') -%}
    {%- endfor -%}

    {%- set result_batches = [] -%}
    {%- for batch in partitions_batches -%}
        {%- if batch -%}
            {%- do result_batches.append(batch | join(' or ')) -%}
        {%- endif -%}
    {%- endfor -%}

    {{ return(result_batches) }}
{%- endmacro %}
