#!/bin/bash

export `cat config` && python3.7 core/social_agent.py &> agent_errors.logs &
python3.7 -m http.server 80 --directory web &> http_errors.logs &
echo 'starting services..'
sleep 5
echo 'put data..'
python3.7 core/put_data.py