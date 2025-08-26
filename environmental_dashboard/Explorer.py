import json
import math
import statistics

with open('data.json','r') as f:
    imdata = json.load(f)

#for i in imdata['value']['timeSeries'][0]['values'][0]:
print(len(imdata['value']['timeSeries'][0]['values'][0]['value']))
data = imdata['value']['timeSeries'][0]['values'][0]['value']

cfs_data = []
for item in data:
    for key, value in item.items():
        print(key,value)
        if key == 'value':
            cfs_data.append(float(value))

print(cfs_data)
print(statistics.mean(cfs_data))


