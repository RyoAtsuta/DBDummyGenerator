import MySQLdb, re
from faker import Faker
from jinja2 import Environment, Template, FileSystemLoader

class DBDummyGenerator:
  def __init__(self, setting):
    self.setting = setting

  def __connect(self, database_name):
    conn = MySQLdb.connect(
      host=self.setting.get('database').get('host'),
      user=self.setting.get('database').get('user'),
      passwd=self.setting.get('database').get('pass'),
      port=self.setting.get('database').get('port'),
      db=database_name,
      charset='utf8'
    )
    return conn

  def __extract_table_columns(self, rows):
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

  def __parse_int(self, column_type, data_type):
    return int(re.match(r'{}\((\d*)\)'.format(data_type), column_type)[1])

  def __insert_values(self, columns):
    faker = Faker()
    values = []
    for column in columns:
      if column['data_type'] == 'time':
        values.append(faker.time())
      if column['data_type'] == 'tinyint':
        digit = self.__parse_int(column['column_type'], 'int')
        values.append(faker.pydecimal(digit//2+1, 0))
      if column['data_type'] == 'mediumtext':
        values.append(faker.name())
      if column['data_type'] == 'longtext':
        values.append(faker.name())
      if column['data_type'] == 'date':
        values.append(faker.date())
      if column['data_type'] == 'varchar':
        length = self.__parse_int(column['column_type'], 'varchar')
        values.append(faker.name()[0:length])
      if column['data_type'] == 'decimal':
        values.append(faker.pydecimal(9, 0))
      if column['data_type'] == 'datetime':
        values.append(faker.date_time().strftime('%Y-%m-%d %H:%M:%S'))
      if column['data_type'] == 'char':
        values.append(faker.name())
      if column['data_type'] == 'bigint':
        digit = self.__parse_int(column['column_type'], 'int')
        values.append(faker.faker.pydecimal(digit//2+1, 0))
      if column['data_type'] == 'float':
        values.append(faker.pydecimal(3, 3))
      if column['data_type'] == 'text':
        values.append(faker.name())
      if column['data_type'] == 'int':
        digit = self.__parse_int(column['column_type'], 'int')
        values.append(faker.pydecimal(digit//2+1, 0))
    return values

  def execute(self):
    env = Environment(loader=FileSystemLoader('./', encoding='utf8'))
    insert_template = env.get_template('insert.tpl')
    insert_sql = ''

    for database_name in self.setting.get('database').get('names'):

      print('[ INFO ] ====================== insert dummy data into "{}" ======================'.format(database_name))

      conn = self.__connect(database_name)
      cursor = conn.cursor()
      cursor.execute('select table_name, column_name, data_type, column_key, column_type from information_schema.columns where table_schema = "{}"'.format(database_name))
      rows = cursor.fetchall()

      table_columns = self.__extract_table_columns(rows)

      progress_current = 0
      progress_max = len(table_columns.keys())
      for table_name, columns in table_columns.items():
        failed_insert_count = 0
        for i in range(self.setting.get('generator').get('count')):
          try:
            values = self.__insert_values(columns)

            params = {
              'table_name': table_name,
              'values': values
            }

            insert_sql = insert_template.render(params)

            cursor.execute(insert_sql)
          except Exception as e:
            failed_insert_count += 1
            if self.setting.get('logging').get('debug'):
              with open(self.setting.get('logging').get('path'), mode='a') as f:
                print('[ ERROR ] {}, table_name = {}, insert_sql = \n\n{}\n\n'.format(e, table_name, insert_sql), file=f)

        progress_current += 1
        conn.commit()
        print('[ INFO ][{}/{}] finished insert {} records: table_name = {}'.format(progress_current, progress_max, self.setting.get('generator').get('count') - failed_insert_count, table_name))

      cursor.close()
      conn.close()
