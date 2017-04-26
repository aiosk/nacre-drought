import requests
import json
from time import sleep
import os
import sys
import copy


def getData(url=''):
    print('processing {url}'.format(url=url))

    data = requests.get(url=url).json()
    return data['data']


def getLocation(url):
    loc = url.split('/')
    return {'kabkota': loc[-5], 'kecamatan': loc[-4], 'kelurahan': loc[-3]}

dataFileName = 'data.json'
data = []
with open(dataFileName) as f:
    data = json.load(f)
    f.close()

urlFileName = 'urls.json'
urlData = []
with open(urlFileName) as f:
    urlData = json.load(f)
    urlDataWrite = copy.deepcopy(urlData)
    f.close()

try:
    for urlVal in urlData:
        # sleep(1)
        tps = getData(urlVal)

        for pemilih in tps:
            if not any(dataPemilih['id'] == pemilih['id'] for dataPemilih in data):
                pemilihLocation = getLocation(urlVal.replace('%20', ' '))
                pemilih.update(pemilihLocation)

                data.append(pemilih)
                print('append {nik} {nama} to data'.format(
                    nik=pemilih['nik'], nama=pemilih['nama']))
            else:
                print('{nik} {nama} already exists'.format(
                    nik=pemilih['nik'], nama=pemilih['nama']))

        print('removing {url} from list'.format(url=urlVal))
        urlDataWrite.remove(urlVal)
except KeyboardInterrupt:
    sys.exit()
finally:
    print('write to {fileName}'.format(fileName=dataFileName))
    with open(dataFileName, 'w') as f:
        json.dump(data, f, indent=2)
        f.close()

    print('write to {fileName}'.format(fileName=urlFileName))
    with open(urlFileName, 'w') as f:
        json.dump(urlDataWrite, f, indent=2)
        f.close()
