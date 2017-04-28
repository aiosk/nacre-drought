import requests
import sqlite3
# # from time import sleep
# # import sys

conn = sqlite3.connect('dpt.db')
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS suara (
    putaran INTEGER NOT NULL,
    totalSuara INTEGER NOT NULL,
    penggunaSuara INTEGER NOT NULL,
    suaraSah INTEGER NOT NULL,
    _suaraTidakSah INTEGER NOT NULL,
    tpsId INTEGER NOT NULL UNIQUE,
    FOREIGN KEY (tpsId) REFERENCES tps(id)
)''')

import lxml.html
from lxml.cssselect import CSSSelector
urlData = set(r[0] for r in c.execute( 'SELECT url FROM _sourceHasil WHERE isFetched=0').fetchall())
rowTps = c.execute('''SELECT t.id,t.nama,t.kelurahanId,k.nama kelurahanNama
    FROM tps t
    join kelurahan k on k.id=t.kelurahanId''').fetchall()
try:
    for urlVal in urlData:
        page = requests.get(url=urlVal)
        page = lxml.html.fromstring(page.content)
        tdSelector = CSSSelector('#rekapHasilPilkada td.text-center td.text-right')

        urlSplit = urlVal.split('/')
        putaran = urlSplit[-6]
        kelurahanNama = urlSplit[-1].upper().replace('_',' ')
        for i,tdElement in enumerate(tdSelector(page)):
            try:
                if i%6==0:
                    totalSuara= tdElement.text
                if i%6==1:
                    penggunaSuara= tdElement.text
                if i%6==3:
                    suaraSah= tdElement.text
                if i%6==4:
                    _suaraTidakSah= tdElement.text

                if ((i+1)/6).is_integer():
                    tps= int(i/6)+1
                    tpsId = tuple(r[0] for r in rowTps if tps == r[1] and kelurahanNama == r[3])[0]
                    c.execute('''INSERT INTO
                        suara  (putaran, totalSuara, penggunaSuara, suaraSah, _suaraTidakSah, tpsId)
                        VALUES (      ?,          ?,             ?,        ?,              ?,     ?)''', (putaran, totalSuara, penggunaSuara, suaraSah, _suaraTidakSah, tpsId,))
                    print('Insert {kelurahanNama} > {tps} to data suara'.format(kelurahanNama=kelurahanNama,tps=tps))
            except sqlite3.IntegrityError as e:
                if 'unique constraint' in e.args[0].lower():
                    print('{kelurahanNama} > {tps} already exists'.format(kelurahanNama=kelurahanNama,tps=tps))
                else:
                    raise

        c.execute('UPDATE _sourceHasil set isFetched=1 where url=?', (urlVal,))
except KeyboardInterrupt:
    pass
finally:
    print('commit suara to database')
    conn.commit()
    conn.close()
