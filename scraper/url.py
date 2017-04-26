import requests
import json


def getUrl(location=''):
    return 'https://pilkada2017.kpu.go.id/pemilih/dpt/2/DKI%20JAKARTA/{location}listDps.json'.format(
        location=location).replace(' ', '%20')


def getData(location=''):
    url = getUrl(location)

    print('fetching /{location}'.format(location=location))
    data = requests.get(url=url).json()

    return data['aaData'] if 'aaData' in data else data['data']

data = []

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
                url = getUrl(location)
                print(url)
                data.append(url)

dataFileName = 'urls.json'
print('write to {fileName}'.format(fileName=dataFileName))
with open(dataFileName, 'w') as f:
    json.dump(data, f, indent=2)
    f.close()
