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
    {%- set ns = namespace(bucket_conditions={}, bucket_column=None) -%}

    {%- for row in rows -%}
        {%- set single_partition = [] -%}
        {%- set partition_conditions = [] -%}
        {%- set bucket_conditions = [] -%}

        {%- for col, partition_key in zip(row, partitioned_by) -%}
            {%- set column_type = adapter.convert_type(table, loop.index0) -%}
            {%- set bucket_match = modules.re.search('bucket\((.+),.+([0-9]+)\)', partition_key) -%}

            {%- if bucket_match -%}
                {%- set ns.bucket_column = bucket_match[1] -%}
                {%- set bucket_num = adapter.murmur3_hash(col, bucket_match[2] | int) -%}
                {%- set formatted_value = adapter.format_value_for_partition(col, column_type) -%}

                {% if bucket_num not in ns.bucket_conditions %}
                    {% do ns.bucket_conditions.update({bucket_num: [formatted_value]}) %}
                {% else %}
                    {% do ns.bucket_conditions[bucket_num].append(formatted_value) %}
                {% endif %}
            {%- else -%}
                {%- set value = adapter.format_value_for_partition(col, column_type) -%}
                {%- set partition_key_formatted = adapter.format_one_partition_key(partitioned_by[loop.index0]) -%}
                {%- do partition_conditions.append(partition_key_formatted + " = " + value) -%}
            {%- endif -%}
        {%- endfor -%}

        {%- set partition_expression = partition_conditions | join(' and ') -%}

        {%- for bucket_num, values in ns.bucket_conditions.items() -%}
            {%- set bucket_condition = ns.bucket_column + " IN (" + values | unique | join(", ") + ")" -%}
            {%- do bucket_conditions.append(bucket_condition) -%}
        {%- endfor -%}

        {%- for condition in bucket_conditions -%}
            {%- do single_partition.append('(' + partition_expression + ' and ' + condition + ')') -%}
        {%- endfor -%}

        {%- if not bucket_conditions -%}
            {%- do single_partition.append('(' + partition_expression + ')') -%}
        {%- endif -%}

        {%- set batch_number = (loop.index0 / athena_partitions_limit) | int -%}
        {%- if batch_number not in partitions_batches %}
            {% do partitions_batches.append([]) %}
        {% endif %}
        {%- do partitions_batches[batch_number].extend(single_partition) -%}
    {%- endfor -%}

    {%- set result_batches = [] -%}
    {%- for batch in partitions_batches -%}
        {%- if batch -%}
            {%- do result_batches.append(batch | join(' or ')) -%}
        {%- endif -%}
    {%- endfor -%}

    {{ return(result_batches) }}
{%- endmacro %}
