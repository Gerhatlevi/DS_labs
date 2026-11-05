#!/bin/bash

# first test uploading a file
echo "testing" > test.txt
curl localhost:5555/test_file.txt -F "file=@test.txt" -v

# then ensure the file can be read
curl localhost:5555/test_file.txt -v
