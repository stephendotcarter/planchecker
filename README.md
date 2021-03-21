# PlanChecker
PlanChecker parses Greenplum query plans and highlight any potential performance issues.

## Package
plan.go contains all the logic for parsing the query plans.
There are 3 example programs which initialize a plan object using different methods.
Test data is in the testdata directory.

### Example reading from file
Passes the filename to PlanChecker
```
./plancheck_example_from_file testdata/explain01.txt
```

### Example reading from string
Reads file contents and passes string to PlanChecker
```
./plancheck_example_from_string testdata/explain01.txt
```

### Example reading from stdin
PlanChecker reads directly from stdin
```
cat testdata/explain01.txt | ./plancheck_example_from_stdin
```

## Webservice
This provides a web interface.
A Postgres database is required.

Create the required table:
```
psql -h HOSTNAME -U USERNAME -d DATABASE -f planchecker.sql
```

Set the env variables:
```
export PORT=8000
export CONSTRING='postgres://USERNAME:PASSWORD@HOSTNAME/DATABASE'
```

Start the service:
```
./planchecker
```

Then browse to:
```
http://localhost:8000
```

## Docker
```
docker build -t stephendotcarter/planchecker:latest .
```