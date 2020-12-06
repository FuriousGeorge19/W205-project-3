#!/bin/bash

docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/
docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/purchase_a_sword/broadsword
docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/purchase_a_sword/longsword
docker-compose exec mids ab -n 10 -H "Host: user1.comcast.com" http://localhost:5000/purchase_a_sword/scimitar


docker-compose exec mids ab -n 10 -H "Host: user2.att.com" http://localhost:5000/join_a_guild/brewers
docker-compose exec mids ab -n 10 -H "Host: user2.att.com" http://localhost:5000/join_a_guild/masons
docker-compose exec mids ab -n 10 -H "Host: user2.att.com" http://localhost:5000/join_a_guild/assassins


docker-compose exec mids ab -n 10 -H "Host: user2.att.com" http://localhost:5000/purchase_armor

docker-compose exec mids ab -n 10 -H "Host: user2.att.com" http://localhost:5000/

