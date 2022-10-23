import MySQLdb, re, yaml
from faker import Faker
from jinja2 import Environment, Template, FileSystemLoader

env = Environment(loader=FileSystemLoader('./', encoding='utf8'))
insert_template = env.get_template('insert.tpl')

faker = Faker()

def get_setting(file_name):
  setting = None

  with open(file_name) as f:
    setting = yaml.safe_load(f)

  return setting

def connect(database_name, database_setting):
  conn = MySQLdb.connect(
    host=database_setting.get('host'),
    user=database_setting.get('user'),
    passwd=database_setting.get('pass'),
    port=database_setting.get('port'),
    db=database_name,
    charset='utf8'
  )
  return conn

def extract_table_columns(rows):
  table_columns = {}
  for row in rows:
    table_name = row[0]
    column_name = row[1]
    data_type = row[2]
    column_key = row[3]
    column_type = row[4]

    if not table_name in table_columns.keys():
      table_columns[table_name] = []
    table_columns[table_name].append({
      'name': column_name,
      'data_type': data_type,
      'column_key': column_key,
      'column_type': column_type
    })
  return table_columns

def parse_int(column_type, data_type):
  return int(re.match(r'{}\((\d*)\)'.format(data_type), column_type)[1])

def insert_values(columns):
  values = []
  for column in columns:
    if column['data_type'] == 'time':
      values.append(faker.time())
    if column['data_type'] == 'tinyint':
      digit = parse_int(column['column_type'], 'int')
      values.append(faker.pydecimal(digit//2+1, 0))
    if column['data_type'] == 'mediumtext':
      values.append(faker.name())
    if column['data_type'] == 'longtext':
      values.append(faker.name())
    if column['data_type'] == 'date':
      values.append(faker.date())
    if column['data_type'] == 'varchar':
      length = parse_int(column['column_type'], 'varchar')
      values.append(faker.name()[0:length])
    if column['data_type'] == 'decimal':
      values.append(faker.pydecimal(9, 0))
    if column['data_type'] == 'datetime':
      values.append(faker.date_time().strftime('%Y-%m-%d %H:%M:%S'))
    if column['data_type'] == 'char':
      values.append(faker.name())
    if column['data_type'] == 'bigint':
      digit = parse_int(column['column_type'], 'int')
      values.append(faker.faker.pydecimal(digit//2+1, 0))
    if column['data_type'] == 'float':
      values.append(faker.pydecimal(3, 3))
    if column['data_type'] == 'text':
      values.append(faker.name())
    if column['data_type'] == 'int':
      digit = parse_int(column['column_type'], 'int')
      values.append(faker.pydecimal(digit//2+1, 0))
  return values

def main(setting):
  for database_name in setting.get('database').get('names'):

    print('[ INFO ] ====================== insert dummy data into "{}" ======================'.format(database_name))

    conn = connect(database_name, setting.get('database'))
    cursor = conn.cursor()
    cursor.execute('select table_name, column_name, data_type, column_key, column_type from information_schema.columns where table_schema = "{}"'.format(database_name))
    rows = cursor.fetchall()

    table_columns = extract_table_columns(rows)

    progress_current = 0
    progress_max = len(table_columns.keys())
    for table_name, columns in table_columns.items():
      failed_insert_count = 0
      for i in range(setting.get('generator').get('count')):
        try:
          values = insert_values(columns)

          params = {
            'table_name': table_name,
            'values': values
          }

          insert_sql = insert_template.render(params)

          cursor.execute(insert_sql)
        except Exception as e:
          failed_insert_count += 1
          if setting.get('logging').get('debug'):
            with open(setting.get('logging').get('path'), mode='a') as f:
              print('[ ERROR ] {}, table_name = {}, insert_sql = \n\n{}\n\n'.format(e, table_name, insert_sql), file=f)

      progress_current += 1
      conn.commit()
      print('[ INFO ][{}/{}] finished insert {} records: table_name = {}'.format(progress_current, progress_max, setting.get('generator').get('count') - failed_insert_count, table_name))

    cursor.close()
    conn.close()

if __name__ == '__main__':
  setting = get_setting('setting.yml')
  main(setting)