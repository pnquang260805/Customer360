#!/bin/bash
set -e
until mongosh --host $MONGO_PRIMARY_HOST --port $MONGO_PRIMARY_PORT --eval "db.runCommand({ping: 1})" --quiet; 
do
    echo "Waiting for connection ..."
    sleep 5
done

mongosh --host $MONGO_PRIMARY_HOST --port $MONGO_PRIMARY_PORT -u $MONGO_USERNAME -p $MONGO_PASSWORD << EOF
    var conf = {
        _id: "$MONGO_INITDB_REPLICASET",
        members: [
            {_id: 0, host: '$MONGO_PRIMARY_HOST:$MONGO_PRIMARY_PORT'},
            {_id: 1, host: '$MONGO_SECONDARY_1_HOST:$MONGO_SECONDARY_1_PORT'},
            {_id: 2, host: '$MONGO_SECONDARY_2_HOST:$MONGO_SECONDARY_2_PORT'},
        ]
    }

    rs.initiate(conf)
    rs.status()
EOF

echo "done"