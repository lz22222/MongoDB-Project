
GROUP PROJECT

2024.4

Group member names:

lz22222
,mingbao  ,qinan2

# Group work break-down strategy
We divide all works equally, the details are provided below:


lz22222 handled the initial setup and data import for the project by developing task1_build.py. Furthermore, Ling Zhuo compiled a comprehensive project report, detailing the findings to ensure clarity and coherence in the documentation.

Mingbao was responsible for the development and optimization of queries in task1_query.py and task2_query.py, which included crafting queries, indexing, and conducting performance analysis post-indexing.


qinan2 focused on implementing task2_build.py, which involved incorporating data from JSON files into MongoDB in an embedded format. Qinan Song also ensured that the database was built in a timely manner and undertook the task of documenting the script's usage and dependencies.

# Code execution guide
Install the PyMongo library if it doesn't exist：
```bash
pip3 install pymongo
```
For Lab Machine, run the following to connect to mongodb server:
```bash
mkdir ~/mongodb_data_folder
```
```bash
mongod --port 27012 --dbpath ~/mongodb_data_folder &
```
```bash
mongosh --port 27012
```
- Connect to lab machine on another terminal and navigate to the folder:
  ```bash
  cd path/to/w24-mp2-nosqlnav
  ```
    - Run each steps for task 1 and task 2 with the following format（change port number to 27012）:
    ```bash
    python3 task1_build.py portnumber
    ```
    ```bash
    python3 task1_query.py portnumber
    ```
    ```bash
    python3 task2_build.py portnumber
    ```
    ```bash
    python3 task2_query.py portnumber
    ```

For MAC users, run the following script:  
- To run mongod manually as a background process using a config file, run:
    - For macOS running Intel processors:
    ```bash
    mongod --config /usr/local/etc/mongod.conf --fork
    ```
    - For macOS running on Apple Silicon processors:
    ```bash
    mongod --config /opt/homebrew/etc/mongod.conf --fork
    ```
- Connect and Use MongoDB
  ```bash
  mongosh
  ```
  - Run each steps for task 1 and task 2 with the following format:
    ```bash
    python3 task1_build.py portnumber
    ```
    ```bash
    python3 task1_query.py portnumber
    ```
    ```bash
    python3 task2_build.py portnumber
    ```
    ```bash
    python3 task2_query.py portnumber
    ```



# AI Agents
- We did not use any AI Agents.


# Collaborations
- We did not collaborate with anyone.  
