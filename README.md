# england-crime
will analyse UK's crime data

Start mongo for testing:
sudo docker-compose -f stack.yml up

Add mongo user:

use spark
db.createUser(
  {
    user: "spark_user",
    pwd: "xyz123",
    roles: [ { role: "readWrite", db: "spark" } ],
    mechanisms:[  "SCRAM-SHA-1" ]
  }
)