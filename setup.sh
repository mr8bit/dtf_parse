virtualenv venv
sleep 1
touch bad_index.txt
source venv/bin/activate
pip install -r requirements.txt
