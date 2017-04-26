import requests
import json
from time import sleep
import os
import sys


def getData(location=''):
    url = 'https://pilkada2017.kpu.go.id/pemilih/dpt/2/DKI%20JAKARTA/{location}listDps.json'.format(
        location=location).replace(' ', '%20')

    print('fetching /{location}'.format(location=location))

    data = requests.get(url=url).json()
    return data['aaData'] if 'aaData' in data else data['data']

dataFileName = 'data.json'
data = []
with open(dataFileName) as f:
    data = json.load(f)
    f.close()
try:
    propinsi = getData()
    for kabKotaVal in propinsi:
        location = '{namaKabKota}/'.format(
            namaKabKota=kabKotaVal['namaKabKota'])
        kabKota = getData(location)

        for kecamatanVal in kabKota:
            location = '{namaKabKota}/{namaKecamatan}/'.format(
                namaKabKota=kabKotaVal['namaKabKota'], namaKecamatan=kecamatanVal['namaKecamatan'])
            kecamatan = getData(location)

            for kelurahanVal in kecamatan:
                location = '{namaKabKota}/{namaKecamatan}/{namaKelurahan}/'.format(namaKabKota=kabKotaVal[
                    'namaKabKota'], namaKecamatan=kecamatanVal['namaKecamatan'], namaKelurahan=kelurahanVal['namaKelurahan'])
                kelurahan = getData(location)

                for tpsVal in kelurahan:
                    location = '{namaKabKota}/{namaKecamatan}/{namaKelurahan}/{tps}/'.format(namaKabKota=kabKotaVal[
                        'namaKabKota'], namaKecamatan=kecamatanVal['namaKecamatan'], namaKelurahan=kelurahanVal['namaKelurahan'], tps=tpsVal['tps'])
                    # sleep(1)
                    tps = getData(location)

                    for pemilih in tps:
                        if not any(dataPemilih['id'] == pemilih['id'] for dataPemilih in data):
                            pemilih['kelurahan'] = kelurahanVal[
                                'namaKelurahan']
                            pemilih['kecamatan'] = kecamatanVal[
                                'namaKecamatan']
                            pemilih['kabkota'] = kabKotaVal['namaKabKota']

                            data.append(pemilih)
                            print('append {nik} {nama} to data'.format(
                                nik=pemilih['nik'], nama=pemilih['nama']))
                        else:
                            print('{nik} {nama} already exists'.format(
                                nik=pemilih['nik'], nama=pemilih['nama']))

except KeyboardInterrupt:
    pass
finally:
    print('write to {fileName}'.format(fileName=dataFileName))
    with open(dataFileName, 'w') as f:
        json.dump(data, f, indent=2)
        f.close()
    sys.exit()
