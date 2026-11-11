#!/bin/bash

# first test uploading a file
rm test.txt || echo "file not exists"
for i in {1..10000}
do
	echo "[$i] test" >> test.txt
done

# start many simultaneous POST uploads to ensure writer consistency
for i in {1..1}
do
    curl localhost:5555/test_file.txt -F "file=@test.txt" -v &
done

# then ensure the file can be read
curl localhost:5555/test_file.txt > get_file.txt

# Compare the outputs of the files
echo "========================================="
echo "Comparing original file to POSTed file"
diff test.txt files/test_file.txt && echo "Files match"

echo "========================================="
echo "Comparing original file to GETed file"
diff test.txt get_file.txt && echo "Files match"
