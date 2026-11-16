#!/bin/bash

# first test uploading a file
rm test.txt || echo "file not exists"
for i in {1..10000}
do
	echo "[$i] test" >> test.txt
done

# start many simultaneous POST uploads to ensure writer consistency
for i in {1..20}
do
    curl localhost:5555/test_file.txt -F "file=@test.txt" &
done

#sleep 1
# then ensure the file can be read
for i in {1..10}
do
    curl localhost:5555/test_file.txt > get_file_${i}.txt
done

# Compare the outputs of the files
echo "========================================="
echo "Comparing original file to POSTed file"
diff test.txt files/test_file.txt && echo "Files match"

echo "========================================="
echo "Comparing original file to GETed file"

for i in {1..10}
do
    diff test.txt get_file_${i}.txt && echo "Files match"
done

BASE_URL="localhost:5555"

echo "=================================="
echo "       PUBSUB TEST SUITE"
echo "=================================="

# 1. Subscribe
echo
echo "[1] Subscribe to topic X"
SUB_ID=$(curl -s "$BASE_URL/subscribe?topic=X" -X POST | jq -r '.id')
echo "    Subscriber UUID: $SUB_ID"

# 2. Publish events
echo
echo "[2] Publish 12 events"
for i in {1..12}; do
    echo "    Publishing event $i..."
    curl -s "$BASE_URL/publish?topic=X" -X POST -d "Hello $i World!" > /dev/null
done
echo "    Done."

# 3. Retrieve events
echo
echo "[3] Retrieve events"
EVENTS_RESP=$(curl -s "$BASE_URL/events?topic=X&id=$SUB_ID")
echo "    Response: $EVENTS_RESP"

# 4. Test blocking behaviour
echo
echo "[4] Test request timeout"
SECONDS=0
curl -s "$BASE_URL/events?topic=X&id=$SUB_ID" > /dev/null
ELAPSED=$SECONDS
echo "    Request duration: ${ELAPSED}s"

# 5. Test invalid requests
echo
echo "[5] Test invalid requests"

echo "    Nonexistent subscriber:"
echo "    $(curl -s "$BASE_URL/events?topic=X&id=1")"

echo "    Nonexistent topic:"
echo "    $(curl -s "$BASE_URL/events?topic=Y&id=$SUB_ID")"

echo "    Missing topic:"
echo "    $(curl -s -i "$BASE_URL/events?id=$SUB_ID")"

echo "    Invalid publish request:"
echo "    $(curl -s -i "$BASE_URL/publish?topic=X")"

# 6. Unsubscribe
echo
echo "[6] Test unsubscribe"

UNSUB_CORRECT=$(curl -s "$BASE_URL/unsubscribe?topic=X&id=$SUB_ID" -X POST)
echo "    First unsubscribe:"
echo "    $UNSUB_CORRECT"

UNSUB_FALSE=$(curl -s "$BASE_URL/unsubscribe?topic=X&id=$SUB_ID" -X POST)
echo "    Second unsubscribe:"
echo "    $UNSUB_FALSE"

echo
echo "=================================="
echo "          TESTS COMPLETE"
echo "=================================="


echo "Hello World on server!" > test.txt

echo "[1] POST - Upload TXT file (expected 201)"
curl -s -i -X POST \
    -F "file=@test.txt" \
    http://localhost:5555/test.txt

    
echo
echo "[2] GET - Retrieve uploaded TXT file (expected 200)"
curl -s -i http://localhost:5555/test.txt

echo
echo "[3] GET - Nonexistent file (expected 404)"
curl -s -i http://localhost:5555/doesnotexist.txt

echo
echo "[4] GET - Unsupported file extension (expected 400)"
curl -s -i http://localhost:5555/test.pdf

echo
echo "[5] PUT - Unsupported method (expected 501)"
curl -s -i -X PUT http://localhost:5555/test.txt

echo
echo "[6] DELETE - Unsupported method (expected 501)"
curl -s -i -X DELETE http://localhost:5555/test.txt

echo
echo "[7] POST - Missing multipart file (expected 400)"
curl -s -i -X POST http://localhost:5555/test.txt
