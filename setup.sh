!/bin/bash

virtualenv venv
sleep 1
source venv/bin/activate
pip install -r requirements.txt
touch bad_index.txt
