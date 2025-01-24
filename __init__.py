__version__ = '0.3.18'

import sys
import subprocess

ARGS = ('-m', 'pip', 'install', '-U')

try:
    __import__('requests')
except:
    subprocess.check_call([sys.executable, *ARGS, 'requests'])

try:
    __import__('yaml')
except:
    subprocess.check_call([sys.executable, *ARGS, 'pyyaml'])

try:
    __import__('googleapiclient')
except:
    subprocess.check_call([sys.executable, *ARGS, 'google-api-python-client'])

try:
    __import__('google.oauth2')
except:
    subprocess.check_call([sys.executable, *ARGS, 'google-auth'])

try:
    __import__('httplib2')
except:
    subprocess.check_call([sys.executable, *ARGS, 'httplib2'])
