import requests
import sqlite3

conn = sqlite3.connect('dpt.db')
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS source (
    url VARCHAR(160) NOT NULL UNIQUE ON CONFLICT IGNORE
)''')
c.execute('''CREATE TABLE IF NOT EXISTS kelurahan (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nama varchar(100) NOT NULL UNIQUE ON CONFLICT IGNORE
)''')
c.execute('''CREATE TABLE IF NOT EXISTS kecamatan (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nama varchar(100) NOT NULL UNIQUE ON CONFLICT IGNORE
)''')
c.execute('''CREATE TABLE IF NOT EXISTS kabkota (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nama varchar(100) NOT NULL UNIQUE ON CONFLICT IGNORE
)''')


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
    c.execute('INSERT INTO kabkota (nama) VALUES (?)',
              (kabKotaVal['namaKabKota'].strip().upper(),))
    kabKota = getData(location)

    for kecamatanVal in kabKota:
        location = '{namaKabKota}/{namaKecamatan}/'.format(
            namaKabKota=kabKotaVal['namaKabKota'], namaKecamatan=kecamatanVal['namaKecamatan'])
        c.execute('INSERT INTO kecamatan (nama) VALUES (?)',
                  (kecamatanVal['namaKecamatan'].strip().upper(),))
        kecamatan = getData(location)

        for kelurahanVal in kecamatan:
            location = '{namaKabKota}/{namaKecamatan}/{namaKelurahan}/'.format(namaKabKota=kabKotaVal[
                'namaKabKota'], namaKecamatan=kecamatanVal['namaKecamatan'], namaKelurahan=kelurahanVal['namaKelurahan'])
            c.execute('INSERT INTO kelurahan (nama) VALUES (?)',
                      (kelurahanVal['namaKelurahan'].strip().upper(),))
            kelurahan = getData(location)

            for tpsVal in kelurahan:
                location = '{namaKabKota}/{namaKecamatan}/{namaKelurahan}/{tps}/'.format(namaKabKota=kabKotaVal[
                    'namaKabKota'], namaKecamatan=kecamatanVal['namaKecamatan'], namaKelurahan=kelurahanVal['namaKelurahan'], tps=tpsVal['tps'])
                url = getUrl(location)
                print(url)
                data.append((url,))

c.executemany('INSERT INTO source (url) VALUES (?)', data)
conn.commit()
conn.close()
