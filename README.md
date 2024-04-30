# Social agent

## Description
This script is designed to automate the collection of data in the social network vk.com.

### Input data
List of users and community id's. Edit `agent/data/groups.txt and agent/data/users.txt` to setup.

### Output data
Ontology in owl format.

## Installation
1.  You need to copy and install the archive on the server on which the agent will be running
    ```sh
    wget https://raw.githubusercontent.com/KateMarmysh/social_agent/new_version_2024/social_agent_vk_onto.zip
    tar -xzf social_agent_vk_onto.zip
    ```
2.  Install python libs (python 3.10 required)
    ```sh
    cd agent
    ./install.sh
    ```
3.  Configure config file with your social token
4.  Run script
    ```sh
    ./run.sh
    ```
