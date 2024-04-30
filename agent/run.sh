#!/bin/bash

export `cat config` && python3.10 core/social_agent.py &> agent_errors.logs &
echo "Starting services..."
echo "Put data..."
export `cat config` && python3.10 core/get_groups_subscribers.py && python3.10 core/put_data.py
echo "Starting an ontology consumer..."