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
    {%- set ns = namespace(partition_conditions={}) -%}

    {%- for row in rows -%}
        {%- set partition_condition = [] -%}
        {%- for col, partition_key in zip(row, partitioned_by) -%}
            {%- set column_type = adapter.convert_type(table, loop.index0) -%}
            {%- set comp_func = '=' -%}
            {%- set bucket_match = modules.re.search('bucket\((.+),.+([0-9]+)\)', partition_key) -%}
            {%- if bucket_match -%}
                {%- set bucket_column = bucket_match[1] -%}
                {%- set bucket_num = adapter.murmur3_hash(col, bucket_match[2] | int) -%}
                {%- set formatted_value = adapter.format_value_for_partition(col, column_type) -%}
                {%- set bucket_key = bucket_column + '_' + bucket_num|string -%}
                {%- if bucket_key not in ns.partition_conditions %}
                    {% do ns.partition_conditions.update({bucket_key: [formatted_value]}) %}
                {%- elif formatted_value not in ns.partition_conditions[bucket_key] -%}
                    {%- do ns.partition_conditions[bucket_key].append(formatted_value) -%}
                {%- endif -%}
            {%- else -%}
                {%- set value = adapter.format_value_for_partition(col, column_type) -%}
                {%- set partition_key_formatted = adapter.format_one_partition_key(partition_key) -%}
                {%- do partition_condition.append(partition_key_formatted + comp_func + value) -%}
            {%- endif -%}
        {%- endfor -%}

        {%- for bucket_key, values in ns.partition_conditions.items() -%}
            {%- do partition_condition.append(bucket_key.split('_')[0] + " IN (" + values | join(", ") + ")") -%}
        {%- endfor -%}

        {%- set single_partition_expression = partition_condition | join(' and ') -%}
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
