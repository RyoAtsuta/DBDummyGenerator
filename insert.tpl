insert into
  {{ table_name }}
values (
  {%- for value in values %}
    {%- if loop.first %}
      '{{ value }}'
    {%- else %}
      ,'{{ value }}'
    {%- endif %}
  {%- endfor %}
)