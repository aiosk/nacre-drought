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
dataCheckExist = tuple(r[0] for r in c.execute('select id from pemilih').fetchall())
rowKelurahan = c.execute('select id,nama from kelurahan').fetchall()
rowKecamatan = c.execute('select id,nama from kecamatan').fetchall()
rowKabkota = c.execute('select id,nama from kabkota').fetchall()
rowJenisKelamin = c.execute('select id,nama from kelamin').fetchall()
rowTempatLahir = c.execute('select id,nama from tempatLahir').fetchall()
urlData = (r[0] for r in c.execute('select url from source').fetchall())


def getData(url=''):
    print('processing {url}'.format(url=url))

    data = requests.get(url=url).json()
    return data['data']


def getTempatLahir(tempatLahirPemilih):
    tempatLahir = [r[0] for r in rowTempatLahir if tempatLahirPemilih.upper() == r[1]]
    if not tempatLahir:
        tempatLahir = c.execute('''INSERT INTO tempatLahir (nama) VALUES (?)''', (tempatLahirPemilih.upper(),)).lastrowid
    else:
        tempatLahir = tempatLahir[0]

    return tempatLahir


def preparePemilih(pemilih, url):
    jenisKelamin = [r[0] for r in rowJenisKelamin if pemilih['jenisKelamin'].lower() == r[1]][0]
    tempatLahir = getTempatLahir(pemilih['tempatLahir'])

    location = url.split('/')
    kelurahan = [r[0] for r in rowKelurahan if location[-3].upper() == r[1]][0]
    kecamatan = [r[0] for r in rowKecamatan if location[-4].upper() == r[1]][0]
    kabkota = [r[0] for r in rowKabkota if location[-5].upper() == r[1]][0]

    return (pemilih['id'], pemilih['nama'].upper(), pemilih['nik'], tempatLahir, pemilih['tps'], pemilih['putaran'], jenisKelamin, kelurahan, kecamatan, kabkota,)


try:
    for urlVal in urlData:
        # sleep(1)
        tps = getData(urlVal)

        for pemilih in tps:
            if pemilih['id'] in dataCheckExist:
                print('Pemilih {nik} {nama} already exists'.format(nik=pemilih['nik'], nama=pemilih['nama']))
            else:
                print('Append {nik} {nama} to data pemilih'.format(nik=pemilih['nik'], nama=pemilih['nama']))
                pemilih = preparePemilih(pemilih, urlVal.replace('%20', ' '))

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

    # sys.exit(0)
