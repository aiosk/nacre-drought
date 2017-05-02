import requests
import sqlite3
# # from time import sleep
# # import sys

conn = sqlite3.connect('dpt.db')
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS suara (
    putaran INTEGER NOT NULL,
    total INTEGER NOT NULL,
    pengguna INTEGER NOT NULL,
    sah INTEGER NOT NULL,
    _tidakSah INTEGER NOT NULL,
    tpsId INTEGER NOT NULL,
    UNIQUE (putaran, tpsId),
    FOREIGN KEY (tpsId) REFERENCES tps(id)
)''')

c.execute('''CREATE TABLE IF NOT EXISTS paslon (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    noUrut INTEGER NOT NULL,
    kepalaDaerah VARCHAR(50) NOT NULL,
    wakilKepalaDaerah VARCHAR(50) NOT NULL,
    UNIQUE (noUrut, kepalaDaerah)
)''')

try:
    data = []
    data.append((1,'Agus Harimurti Yudhoyono, M.Sc., M.P.A., M.A.','Prof. Dr. Hj. Sylviana Murni, S.H.,M.Si.'))
    data.append((2,'Ir. Basuki Tjahaja Purnama, M.M.','Drs. H. Djarot Saiful Hidayat, M.S.'))
    data.append((3,'Anies Baswedan, Ph.D.','Sandiaga Salahuddin Uno'))
    c.executemany('''INSERT INTO
        paslon (noUrut,kepalaDaerah,wakilKepalaDaerah)
        VALUES (     ?,           ?,                ?)''', tuple(data))
except sqlite3.IntegrityError:
    pass

c.execute('''CREATE TABLE IF NOT EXISTS pilihan (
    putaran INTEGER NOT NULL,
    paslonId INTEGER NOT NULL,
    total INTEGER NOT NULL DEFAULT 0,
    tpsId INTEGER NOT NULL,
    UNIQUE (putaran,paslonId, tpsId),
    FOREIGN KEY (tpsId) REFERENCES tps(id),
    FOREIGN KEY (paslonId) REFERENCES paslon(id)
)''')


import lxml.html
from lxml.cssselect import CSSSelector
import re
urlData = set(r[0] for r in c.execute( 'SELECT url FROM _sourceHasil WHERE isFetched=0').fetchall())
rowTps = c.execute('''SELECT t.id,t.nama,t.kelurahanId,k.nama kelurahanNama
    FROM tps t
    join kelurahan k on k.id=t.kelurahanId''').fetchall()

rowPaslon = c.execute('SELECT id,noUrut FROM paslon').fetchall()
paslon1Id=tuple(r[0] for r in rowPaslon if 1 == r[1])[0]
paslon2Id=tuple(r[0] for r in rowPaslon if 2 == r[1])[0]
paslon3Id=tuple(r[0] for r in rowPaslon if 3 == r[1])[0]

try:
    for urlVal in urlData:
        page = requests.get(url=urlVal)
        page = lxml.html.fromstring(page.content)

        print(urlVal)
        urlSplit = urlVal.split('/')

        try:
            putaran = int(urlSplit[-6])
        except ValueError:
            putaran = 1

        kelurahanNama = urlSplit[-1].upper().replace('_',' ')

        tdSelectorSuara = CSSSelector('#rekapHasilPilkada td.text-center td.text-right')
        for i,val in enumerate(tdSelectorSuara(page)):
            try:
                if i%6==0:
                    totalSuara= val.text
                if i%6==1:
                    penggunaSuara= val.text
                if i%6==3:
                    suaraSah= val.text
                if i%6==4:
                    _suaraTidakSah= val.text

                if ((i+1)/6).is_integer():
                    tps= int(i/6)+1
                    tpsId = tuple(r[0] for r in rowTps if tps == r[1] and kelurahanNama == r[3])[0]
                    c.execute('''INSERT INTO
                        suara  (putaran, total, pengguna, sah, _tidakSah, tpsId)
                        VALUES (      ?,          ?,             ?,        ?,              ?,     ?)''', (putaran, totalSuara, penggunaSuara, suaraSah, _suaraTidakSah, tpsId,))
                    print('Insert suara {kelurahanNama} > {tps} to data suara'.format(kelurahanNama=kelurahanNama,tps=tps))
            except sqlite3.IntegrityError as e:
                if 'unique constraint' in e.args[0].lower():
                    print('Suara {kelurahanNama} > {tps} already exists'.format(kelurahanNama=kelurahanNama,tps=tps))
                else:
                    raise

        tdSelectorPilihan = CSSSelector('#rekapHasilPilkada > tbody>tr>td:nth-child(4) script')
        regexPaslonNo1 = re.compile(r".+case 1.+rekapHasil\.push\((\d+)\).+",re.MULTILINE)
        regexPaslonNo2 = re.compile(r".+case 2.+rekapHasil\.push\((\d+)\).+",re.MULTILINE)
        regexPaslonNo3 = re.compile(r".+case 3.+rekapHasil\.push\((\d+)\).+",re.MULTILINE)
        for i,val in enumerate(tdSelectorPilihan(page)):
            try:
                tps=i+1
                tpsId = tuple(r[0] for r in rowTps if tps == r[1] and kelurahanNama == r[3])[0]
                paslon1=int([r.groups() for r in regexPaslonNo1.finditer(val.text)][0][0])
                paslon2=int([r.groups() for r in regexPaslonNo2.finditer(val.text)][0][0])
                paslon3=int([r.groups() for r in regexPaslonNo3.finditer(val.text)][0][0])
                data = [(putaran,paslon1Id,paslon1,tpsId,), (putaran,paslon2Id,paslon2,tpsId,), (putaran,paslon3Id,paslon3,tpsId,)]

                c.executemany('''INSERT INTO
                    pilihan (putaran, paslonId, total, tpsId)
                    VALUES  (      ?,      ?,     ?,     ?)''',tuple(data))
                print('Insert pilihan {kelurahanNama} > {tps} to data pilihan'.format(kelurahanNama=kelurahanNama,tps=tps))
            except sqlite3.IntegrityError as e:
                if 'unique constraint' in e.args[0].lower():
                    print('Pilihan {kelurahanNama} > {tps} already exists'.format(kelurahanNama=kelurahanNama,tps=tps))
                else:
                    raise

        c.execute('UPDATE _sourceHasil SET isFetched=1 WHERE url=?', (urlVal,))
except KeyboardInterrupt:
    pass
finally:
    print('commit suara to database')
    conn.commit()
    conn.close()
