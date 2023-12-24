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
    {%- set ns = namespace(bucket_column=None, bucket_values_map={}) -%}

    {%- for row in rows -%}
        {%- set partition_conditions = [] -%}
        {%- set ns.bucket_values_map = {} -%}

        {%- for col, partition_key in zip(row, partitioned_by) -%}
            {%- set column_type = adapter.convert_type(table, loop.index0) -%}
            {%- if 'bucket(' in partition_key -%}
                {%- set bucket_match = modules.re.search('bucket\((.+),.+([0-9]+)\)', partition_key) -%}
                {%- set ns.bucket_column = bucket_match[1] -%}
                {%- set bucket_num = adapter.murmur3_hash(col, bucket_match[2] | int) -%}
                {%- set formatted_value = adapter.format_value_for_partition(col, column_type) -%}
                {% if bucket_num not in ns.bucket_values_map %}
                    {% do ns.bucket_values_map.update({bucket_num: [formatted_value]}) %}
                {% elif formatted_value not in ns.bucket_values_map[bucket_num] %}
                    {% do ns.bucket_values_map[bucket_num].append(formatted_value) %}
                {% endif %}
            {%- else -%}
                {%- set value = adapter.format_value_for_partition(col, column_type) -%}
                {%- set partition_key_formatted = adapter.format_one_partition_key(partition_key) -%}
                {%- do partition_conditions.append(partition_key_formatted + " = " + value) -%}
            {%- endif -%}
        {%- endfor -%}

        {%- if ns.bucket_column -%}
            {%- for bucket_num, values in ns.bucket_values_map.items() -%}
                {%- set bucket_condition = ns.bucket_column + " IN (" + values | unique | join(", ") + ")" -%}
                {%- do partition_conditions.append(bucket_condition) -%}
            {%- endfor -%}
        {%- endif -%}

        {%- set single_partition_expression = partition_conditions | join(' and ') -%}
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
