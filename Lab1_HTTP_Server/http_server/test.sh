#!/bin/bash

# first test uploading a file
for i in {1..10000}
do
	echo "[$i] test" >> test.txt
done
curl localhost:5555/test_file.txt -F "file=@test.txt" -v &
curl localhost:5555/test_file.txt -F "file=@test.txt" -v &
curl localhost:5555/test_file.txt -F "file=@test.txt" -v &

# then ensure the file can be read
curl localhost:5555/test_file.txt -v
