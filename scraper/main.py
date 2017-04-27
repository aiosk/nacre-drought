import requests
import sqlite3
# from time import sleep
# import sys

conn = sqlite3.connect('dpt.db')
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS pemilih (
    id INTEGER NOT NULL UNIQUE ON CONFLICT IGNORE,
    nama varchar(100) NOT NULL,
    nik varchar(20) NOT NULL,
    tempatLahir varchar(50) NOT NULL,
    tps varchar(4) NOT NULL,
    putaran varchar(2) NOT NULL,
    jenisKelamin INTEGER NOT NULL,
    kelurahan INTEGER NOT NULL,
    kecamatan INTEGER NOT NULL,
    kabkota INTEGER NOT NULL
)''')
c.execute('''CREATE TABLE IF NOT EXISTS kelamin (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nama varchar(100) NOT NULL UNIQUE ON CONFLICT IGNORE
)''')
c.executemany('INSERT INTO kelamin (nama) VALUES (?)',
              (('laki-laki',), ('perempuan',)))
c.execute('''CREATE TABLE IF NOT EXISTS tempatLahir (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  nama varchar(100) NOT NULL UNIQUE ON CONFLICT IGNORE
)''')

# load data to memory
data = []
dataCheckExist = tuple(r[0] for r in c.execute('SELECT id FROM pemilih').fetchall())
rowKelurahan = c.execute('SELECT id,nama FROM kelurahan').fetchall()
rowKecamatan = c.execute('SELECT id,nama FROM kecamatan').fetchall()
rowKabkota = c.execute('SELECT id,nama FROM kabkota').fetchall()
rowJenisKelamin = c.execute('SELECT id,nama FROM kelamin').fetchall()
rowTempatLahir = c.execute('SELECT id,nama FROM tempatLahir').fetchall()
urlData = tuple(r[0] for r in c.execute('SELECT url FROM source').fetchall())


def getData(url=''):
    print('processing {url}'.format(url=url))

    data = requests.get(url=url).json()
    return data['data']


def getTempatLahir(tempatLahirPemilih):
    global rowTempatLahir

    tempatLahir = tuple( r[0] for r in rowTempatLahir if tempatLahirPemilih.upper() == r[1])
    if not tempatLahir:
        tempatLahir = c.execute( 'INSERT INTO tempatLahir (nama) VALUES (?)', (tempatLahirPemilih.upper(),)).lastrowid
        rowTempatLahir = c.execute( 'SELECT id,nama FROM tempatLahir').fetchall()
    else:
        tempatLahir = tempatLahir[0]

    return tempatLahir


def parseLocation(url):
    location = url.split('/')
    kelurahan = tuple(r[0] for r in rowKelurahan if location[-3].upper() == r[1])[0]
    kecamatan = tuple(r[0] for r in rowKecamatan if location[-4].upper() == r[1])[0]
    kabkota = tuple(r[0] for r in rowKabkota if location[-5].upper() == r[1])[0]

    return {'kelurahan': kelurahan, 'kecamatan': kecamatan, 'kabkota': kabkota}


def preparePemilih(pemilih):
    jenisKelamin = tuple(r[0] for r in rowJenisKelamin if pemilih[ 'jenisKelamin'].lower() == r[1])[0]
    tempatLahir = getTempatLahir(pemilih['tempatLahir'])

    return (pemilih['id'], pemilih['nama'].upper(), pemilih['nik'], tempatLahir, pemilih['tps'], pemilih['putaran'], jenisKelamin, pemilih['kelurahan'], pemilih['kecamatan'], pemilih['kabkota'],)


try:
    for urlVal in urlData:
        # sleep(1)
        tps = getData(urlVal)
        tpsLocation = parseLocation(urlVal.replace('%20', ' '))

        for pemilih in tps:
            if pemilih['id'] in dataCheckExist:
                print('Pemilih {nik} {nama} already exists'.format(nik=pemilih['nik'], nama=pemilih['nama']))
            else:
                pemilih.update(tpsLocation)
                print('Append {nik} {nama} to data pemilih'.format(nik=pemilih['nik'], nama=pemilih['nama']))
                pemilih = preparePemilih(pemilih)

            # pemilih.update(tpsLocation)
            # print('Append {nik} {nama} to data pemilih'.format( nik=pemilih['nik'], nama=pemilih['nama']))
            # pemilih = preparePemilih(pemilih)

            # print(pemilih)
            data.append(pemilih)

        print('removing {url} from list'.format(url=urlVal))
        c.execute('DELETE FROM source where url=?', (urlVal,))
except KeyboardInterrupt:
    pass
finally:
    print('saving pemilih to database')
    c.executemany('''INSERT INTO
        pemilih (id, nama, nik, tempatLahir, tps, putaran, jenisKelamin, kelurahan, kecamatan, kabkota)
        VALUES  ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', tuple(data))
    conn.commit()
    conn.close()
