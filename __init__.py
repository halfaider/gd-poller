import sys
import subprocess

try:
    __import__('yaml')
except:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'pyyaml'])

try:
    __import__('requests')
except:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-U', 'requests'])

try:
    __import__('googleapiclient')
except:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'google-api-python-client'])

try:
    __import__('google.oauth2')
except:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', '--upgrade', 'google-auth'])
