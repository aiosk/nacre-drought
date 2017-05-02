import requests
import sqlite3

conn = sqlite3.connect('dpt.db')
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS _sourceHasil (
    url VARCHAR(160) NOT NULL UNIQUE ON CONFLICT IGNORE,
    isFetched BOOLEAN NOT NULL DEFAULT 0,
    kelurahanId INTEGER,
    FOREIGN KEY (kelurahanId) REFERENCES kelurahan(id)
)''')


def getUrl(location=''):
    # return 'https://pilkada2017.kpu.go.id/hasil/2/t1/dki_jakarta/{location}'.format( location=location).replace(' ', '%20')
    return 'https://pilkada2017.kpu.go.id/hasil/t1/dki_jakarta/{location}'.format( location=location).replace(' ', '%20')

rowKelurahan = c.execute('''SELECT kk.nama kabkota,
       kc.nama kecamatan,
       kl.nama kelurahan,
       kl.id kelurahanId
  FROM kelurahan kl
JOIN kecamatan kc ON kc.id = kl.kecamatanId
JOIN kabkota kk ON kk.id = kc.kabkotaId''').fetchall()
data = []
try:
    for val in rowKelurahan:
        namaKecamatan = 'palmerah' if val[1].lower() == 'pal merah' else val[1].lower()
        location = '{namaKabKota}/{namaKecamatan}/{namaKelurahan}'.format(namaKabKota=val[0].lower().replace(' ', '_'), namaKecamatan=namaKecamatan.replace(' ', '_'), namaKelurahan=val[2].lower().replace(' ', '_'))
        print('Append {namaKabKota} > {namaKecamatan} > {namaKelurahan}'.format(namaKabKota=val[0], namaKecamatan=namaKecamatan, namaKelurahan=val[2]))
        data.append((getUrl(location),val[3],))

except KeyboardInterrupt:
    print('Handling KeyboardInterrupt exception')
finally:
    print('saving to database')

    c.executemany('INSERT INTO _sourceHasil (url,kelurahanId) VALUES (?,?)', tuple(data))
    conn.commit()
    conn.close()
