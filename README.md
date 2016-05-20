# planchecker
planchecker consumes a GPDB explain plan and parses the slices which results in a query performance report.

## Details
There are 3 example programs which initialize the Plan object using different methods.
Test data is in the testdata directory.

### Example reading from file
Passes the filename to Plan package
```
./plancheck_example_from_file testdata/explain01.txt
```

### Example reading from string
Reads file contents and passes string to Plan package
```
./plancheck_example_from_string testdata/explain01.txt
```

### Example reading from stdin
Plan package reads directly from stdin
```
cat testdata/explain01.txt | ./plancheck_example_from_stdin
```
