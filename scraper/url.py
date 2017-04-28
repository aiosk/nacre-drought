import requests
import sqlite3

conn = sqlite3.connect('dpt.db')
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS kabkota (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nama varchar(100) NOT NULL UNIQUE
)''')
c.execute('''CREATE TABLE IF NOT EXISTS kecamatan (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nama varchar(100) NOT NULL UNIQUE ,
    kabkotaId INTEGER,
    FOREIGN KEY (kabkotaId) REFERENCES kabkota(id)
)''')
c.execute('''CREATE TABLE IF NOT EXISTS kelurahan (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nama varchar(100) NOT NULL UNIQUE ,
    kecamatanId INTEGER,
    FOREIGN KEY (kecamatanId) REFERENCES kecamatan(id)
)''')
c.execute('''CREATE TABLE IF NOT EXISTS tps (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nama INTEGER NOT NULL,
    kelurahanId INTEGER,
    UNIQUE (nama, kelurahanId),
    FOREIGN KEY (kelurahanId) REFERENCES kelurahan(id)
)''')
c.execute('''CREATE TABLE IF NOT EXISTS _source (
    url VARCHAR(160) NOT NULL UNIQUE ON CONFLICT IGNORE,
    isFetched BOOLEAN NOT NULL DEFAULT 0,
    tpsId INTEGER,
    FOREIGN KEY (tpsId) REFERENCES tps(id)
)''')


def getUrl(location=''):
    return 'https://pilkada2017.kpu.go.id/pemilih/dpt/2/DKI%20JAKARTA/{location}listDps.json'.format( location=location).replace(' ', '%20')


def getData(location=''):
    data = requests.get(url=getUrl(location)).json()

    return data['aaData'] if 'aaData' in data else data['data']


def getLocationId(table, value, fieldParent=None, parentId=None):
    if not fieldParent:
        try:
            rowId = c.execute('INSERT INTO {table} (nama) VALUES (?)'.format( table=table), (value.upper(),)).lastrowid
        except sqlite3.IntegrityError:
            rowId = c.execute('SELECT id FROM {table} WHERE nama=?'.format( table=table), (value.upper(),)).fetchone()[0]
    else:
        try:
            sql = 'INSERT INTO {table} (nama, {fieldParent}) VALUES (?,?)'.format( table=table, fieldParent=fieldParent)
            rowId = c.execute(sql, (value.upper(), parentId,)).lastrowid
        except sqlite3.IntegrityError:
            sql = 'SELECT id FROM {table} WHERE nama=? and {fieldParent}=?'.format( table=table, fieldParent=fieldParent)
            rowId = c.execute(sql, (value.upper(), parentId,)).fetchone()[0]

    return rowId

data = []
try:
    propinsi = getData()
    for kabKotaVal in propinsi:
        location = '{namaKabKota}/'.format( namaKabKota=kabKotaVal['namaKabKota'])
        kabkotaId = getLocationId('kabkota', kabKotaVal[ 'namaKabKota'].strip().upper())

        kabKota = getData(location)
        for kecamatanVal in kabKota:
            location = '{namaKabKota}/{namaKecamatan}/'.format( namaKabKota=kabKotaVal['namaKabKota'], namaKecamatan=kecamatanVal['namaKecamatan'])
            kecamatanId = getLocationId('kecamatan', kecamatanVal[ 'namaKecamatan'].strip().upper(), 'kabkotaId', kabkotaId)

            kecamatan = getData(location)
            for kelurahanVal in kecamatan:
                location = '{namaKabKota}/{namaKecamatan}/{namaKelurahan}/'.format(namaKabKota=kabKotaVal[ 'namaKabKota'], namaKecamatan=kecamatanVal['namaKecamatan'], namaKelurahan=kelurahanVal['namaKelurahan'])
                kelurahanId = getLocationId('kelurahan', kelurahanVal[ 'namaKelurahan'].strip().upper(), 'kecamatanId', kecamatanId)

                kelurahan = getData(location)
                for tpsVal in kelurahan:
                    location = '{namaKabKota}/{namaKecamatan}/{namaKelurahan}/{tps}/'.format(namaKabKota=kabKotaVal['namaKabKota'], namaKecamatan=kecamatanVal[ 'namaKecamatan'], namaKelurahan=kelurahanVal['namaKelurahan'], tps=tpsVal['tps'])

                    try:
                        c.execute('INSERT INTO tps (nama, kelurahanId) VALUES (?, ?)', (tpsVal[ 'tps'], kelurahanId,)).lastrowid
                        print('Append TPS {kabkota} > {kecamatan} > {kelurahan} > {tps}'.format(kabkota=kabKotaVal[ 'namaKabKota'], kecamatan=kecamatanVal['namaKecamatan'], kelurahan=kelurahanVal['namaKelurahan'], tps=tpsVal['tps'],))
                        data.append((getUrl(location), tpsVal['tps'],))
                    except sqlite3.IntegrityError:
                        print('TPS existed {kabkota} > {kecamatan} > {kelurahan} > {tps}'.format(kabkota=kabKotaVal[ 'namaKabKota'], kecamatan=kecamatanVal['namaKecamatan'], kelurahan=kelurahanVal['namaKelurahan'], tps=tpsVal['tps'],))

except KeyboardInterrupt:
    print('Handling KeyboardInterrupt exception')
finally:
    print('saving to database')

    c.executemany('''INSERT INTO
        _source (url, tpsId)
         VALUES (  ?,     ?)''', tuple(data))
    conn.commit()
    conn.close()
