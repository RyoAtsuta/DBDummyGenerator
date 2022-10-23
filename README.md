# DBDummyGenerator
## Description
This tool generates dummy data into databases you specified based on the information_schemas of the databases,
so you don't need manually to add dummy data or to implement a program each time.

## Python Version
```
Python 3.8.12
```

## Install
```
$ make install
```

## Run
```
$ python main.py
```

## Configuration
```
generator:
  count: 100  # The number of records you want to generate

# target databases to insert dummy data into
database:
  names: ['<database1>', '<database2>', '<database3>']
  host: '127.0.0.1'
  user: 'root'
  pass: 'root'
  port: 13306
logging:
  debug: false
  path: error.log  # if debug is set to true, then it outputs errors into log file you specified.
```

## not-fixed-yet bugs
- Unable to insert dummy records into databases as you specified in your configuration
  - Duplicate Key Errors
  - etc...
