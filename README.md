# Social agent

## Description
This script is designed to automate the collection of data in the social network vk.com.

### Input data
List of users and community id's. Edit `agent/core/data/groups.txt and agent/core/data/users.txt` to setup.

### Output data
Process logs in xes format http://www.xes-standard.org. 
Ontology in owl format.

## Installation
1.  You need to copy and install the archive on the server on which the agent will be running
    ```sh
    wget https://raw.githubusercontent.com/Speakerkfm/social_agent/master/agent.tar.gz
    tar -xzf agent.tar.gz
    ```
2.  Install python libs (python 3.7 required)
    ```sh
    cd agent
    ./install
    ```
3.  Configure config file with your social token
4.  Run script
    ```sh
    ./run.sh
    ```