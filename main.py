import yaml
from db_dummy_generator import DBDummyGenerator

def get_setting(file_name):
  setting = None

  with open(file_name) as f:
    setting = yaml.safe_load(f)

  return setting

if __name__ == '__main__':
  setting = get_setting('setting.yml')
  generator = DBDummyGenerator(setting)
  generator.execute()