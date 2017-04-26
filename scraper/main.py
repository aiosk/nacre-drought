import requests


def getData(location=''):
    url = 'https://pilkada2017.kpu.go.id/pemilih/dpt/2/DKI%20JAKARTA/{location}listDps.json'.format(
        location=location).replace(' ', '%20')
    data = requests.get(url=url).json()
    return data['aaData'] if 'aaData' in data else data['data']

data = []
propinsi = getData()
for kabKotaVal in propinsi:
    location = '{namaKabKota}/'.format(namaKabKota=kabKotaVal['namaKabKota'])
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
                tps = getData(location)
                for pemilih in tps:
                    pemilih['kelurahan'] = kelurahanVal['namaKelurahan']
                    pemilih['kecamatan'] = kecamatanVal['namaKecamatan']
                    pemilih['kabkota'] = kabKotaVal['namaKabKota']
                    print(pemilih)
                    data.append(pemilih)

import json
with open('data.json', 'w') as outfile:
    json.dump(data, outfile)
