import json
import urllib.request

d = json.load(open('.private/ai.json'))
req = urllib.request.Request(d['base_url'] + '/models', headers={'Authorization': 'Bearer ' + d['api_key']})
with urllib.request.urlopen(req, timeout=30) as resp:
    data = json.load(resp)
ids = [m.get('id') for m in data.get('data', [])]
grok = [i for i in ids if i and 'grok' in i.lower()]
print(len(ids), 'models total')
for g in grok:
    print(g)
