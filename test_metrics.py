import urllib.request
import sys

try:
    response = urllib.request.urlopen('http://localhost:8001/metrics', timeout=2)
    text = response.read().decode('utf-8')
    for line in text.split('\n'):
        if 'disvent_pipeline' in line:
            print(line)
except Exception as e:
    print(f"Error: {e}")
