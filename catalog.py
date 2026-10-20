#!/usr/bin/env python3
"""Ozonchik open-data catalog: streaming TSV/CSV(.gz) importer + SQLite API."""
import argparse,csv,gzip,json,os,re,sqlite3,time,hashlib
from pathlib import Path
from http.server import ThreadingHTTPServer,BaseHTTPRequestHandler
from urllib.parse import urlparse,parse_qs
ROOT=Path(__file__).resolve().parent
DB=Path(os.environ.get('OZONCHIK_DB_PATH', '/data/catalog.sqlite3' if os.environ.get('RAILWAY_ENVIRONMENT') else str(ROOT/'catalog.sqlite3')))
SOURCES={'food':{'name':'Open Food Facts','license':'ODbL 1.0','image_license':'CC BY-SA 3.0','site':'https://world.openfoodfacts.org','attribution':'© Open Food Facts contributors'},'beauty':{'name':'Open Beauty Facts','license':'ODbL 1.0','image_license':'CC BY-SA 3.0','site':'https://world.openbeautyfacts.org','attribution':'© Open Beauty Facts contributors'}}
SCHEMA='''CREATE TABLE IF NOT EXISTS products (id TEXT PRIMARY KEY,source TEXT NOT NULL,barcode TEXT NOT NULL,name TEXT NOT NULL,brand TEXT,category TEXT,image TEXT NOT NULL,source_url TEXT NOT NULL,price_rub INTEGER NOT NULL,metadata TEXT,created_at INTEGER);CREATE INDEX IF NOT EXISTS ix_products_category ON products(category);CREATE INDEX IF NOT EXISTS ix_products_brand ON products(brand);CREATE TABLE IF NOT EXISTS imports (id INTEGER PRIMARY KEY,source TEXT,filename TEXT,seen INTEGER,accepted INTEGER,skipped INTEGER,created_at INTEGER);'''
def connect():
 DB.parent.mkdir(parents=True,exist_ok=True)
 c=sqlite3.connect(DB,timeout=20);c.execute('PRAGMA busy_timeout=20000');c.executescript(SCHEMA);c.row_factory=sqlite3.Row;return c
def valid_url(url):
 try:
  p=urlparse(url);return p.scheme=='https' and p.hostname and (p.hostname.endswith('.openfoodfacts.org') or p.hostname.endswith('.openbeautyfacts.org') or p.hostname.endswith('.openfoodfacts.net'))
 except: return False
def clean(v):return re.sub(r'\s+',' ',(v or '').strip())[:300]
def image_for(row,source):
 for k in ('image_front_url','image_url','image_front_small_url'):
  if valid_url(row.get(k,'') or ''):return row[k].strip()
 return ''
def row_to_product(row,source):
 code=clean(row.get('code') or row.get('barcode') or '')
 name=clean(row.get('product_name_ru') or row.get('product_name') or row.get('generic_name') or '')
 img=image_for(row,source)
 if not re.fullmatch(r'\d{8,14}',code) or len(name)<3 or not img:return None
 brand=clean(row.get('brands','')).split(',')[0]
 cat='Продукты' if source=='food' else 'Красота'
 tags=clean(row.get('categories_tags','')).lower()
 if source=='food':
  if 'beverage' in tags or 'drink' in tags:cat='Напитки'
  elif 'snack' in tags or 'chocolate' in tags:cat='Снеки и сладости'
  elif 'dair' in tags:cat='Молочные продукты'
  elif 'bread' in tags:cat='Хлеб и выпечка'
 seed=int(hashlib.sha256((source+':'+code).encode()).hexdigest()[:8],16)
 # Fictional price explicitly derived deterministically, never presented as market price.
 price=99+(seed%3000)//10*10
 return (source+':'+code,source,code,name,brand,cat,img,SOURCES[source]['site']+'/product/'+code,price,json.dumps({'quantity':clean(row.get('quantity','')),'categories':clean(row.get('categories','')),'license':SOURCES[source]['license'],'image_license':SOURCES[source]['image_license']},ensure_ascii=False),int(time.time()))
def import_file(path,source,limit=0):
 if source not in SOURCES:raise ValueError('Unknown source')
 opener=gzip.open if str(path).endswith('.gz') else open
 c=connect();seen=accepted=skipped=0
 with opener(path,'rt',encoding='utf-8-sig',errors='replace',newline='') as f:
  sample=f.readline();f.seek(0);delimiter='\t' if '\t' in sample else ','
  reader=csv.DictReader(f,delimiter=delimiter)
  for row in reader:
   seen+=1;item=row_to_product(row,source)
   if item:
    c.execute('INSERT OR IGNORE INTO products VALUES (?,?,?,?,?,?,?,?,?,?,?)',item)
    accepted+=1
   else:skipped+=1
   if seen%2000==0:c.commit();print(f'{source}: processed {seen:,}, qualified {accepted:,}',flush=True)
   if limit and seen>=limit:break
  c.execute('INSERT INTO imports(source,filename,seen,accepted,skipped,created_at) VALUES(?,?,?,?,?,?)',(source,str(path),seen,accepted,skipped,int(time.time())))
  c.commit()
 print(json.dumps({'source':source,'processed':seen,'qualified':accepted,'skipped':skipped,'total_unique':c.execute('SELECT COUNT(*) FROM products').fetchone()[0]},ensure_ascii=False))
 c.close()
class Handler(BaseHTTPRequestHandler):
 def respond(self,obj,status=200):
  b=json.dumps(obj,ensure_ascii=False).encode();self.send_response(status);self.send_header('Content-Type','application/json; charset=utf-8');self.send_header('Access-Control-Allow-Origin','*');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
 def do_GET(self):
  u=urlparse(self.path);q=parse_qs(u.query);c=connect()
  if u.path=='/health':return self.respond({'ok':True,'service':'ozonchik'})
  if u.path=='/api/stats':
   rows=[dict(x) for x in c.execute('SELECT source,COUNT(*) AS count FROM products GROUP BY source')];return self.respond({'total':sum(x['count'] for x in rows),'by_source':rows,'sources':SOURCES,'targets':[10000,100000,1000000]})
  if u.path=='/api/products':
   try:page=max(1,int(q.get('page',['1'])[0]));size=min(60,max(1,int(q.get('size',['24'])[0])))
   except:return self.respond({'error':'Invalid pagination'},400)
   filters=[];args=[]
   for field,key in [('category','category'),('source','source')]:
    if q.get(key):filters.append(field+'=?');args.append(q[key][0][:80])
   if q.get('q'):
    filters.append('(name LIKE ? OR brand LIKE ? OR barcode LIKE ?)');term='%'+q['q'][0][:100].replace('%','').replace('_','')+'%';args.extend([term]*3)
   where=' WHERE '+' AND '.join(filters) if filters else ''
   total=c.execute('SELECT COUNT(*) FROM products'+where,args).fetchone()[0]
   rows=[dict(x) for x in c.execute('SELECT id,source,barcode,name,brand,category,image,source_url,price_rub,metadata FROM products'+where+' ORDER BY id LIMIT ? OFFSET ?',args+[size,(page-1)*size])]
   for row in rows:row['metadata']=json.loads(row['metadata'] or '{}')
   return self.respond({'page':page,'size':size,'total':total,'items':rows})
  if u.path=='/api/by-ids':
   ids=[x for x in q.get('ids',[''])[0].split(',') if re.fullmatch(r'(food|beauty):\d{8,14}',x)][:100]
   if not ids:return self.respond([])
   return self.respond([dict(x) for x in c.execute('SELECT id,name,price_rub,image,source_url FROM products WHERE id IN ('+','.join('?'*len(ids))+')',ids)])
  if u.path=='/api/categories':return self.respond([dict(x) for x in c.execute('SELECT category,COUNT(*) AS count FROM products GROUP BY category ORDER BY count DESC')])
  if u.path=='/legacy':
   b=(ROOT/'legacy_ozonchik.html').read_bytes();self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b);return
  if u.path=='/' or u.path=='/index.html':
   b=(ROOT/'index.html').read_bytes();self.send_response(200);self.send_header('Content-Type','text/html; charset=utf-8');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b);return
  self.respond({'error':'Not found'},404)
 def log_message(self,*args):pass
if __name__=='__main__':
 p=argparse.ArgumentParser();sub=p.add_subparsers(dest='cmd',required=True)
 imp=sub.add_parser('import');imp.add_argument('file');imp.add_argument('--source',choices=list(SOURCES),required=True);imp.add_argument('--limit',type=int,default=0)
 srv=sub.add_parser('serve');srv.add_argument('--port',type=int,default=int(os.environ.get('PORT','8765')))
 st=sub.add_parser('stats');a=p.parse_args()
 if a.cmd=='import':import_file(a.file,a.source,a.limit)
 elif a.cmd=='stats':
  c=connect();print(json.dumps({'total':c.execute('SELECT COUNT(*) FROM products').fetchone()[0],'imports':[dict(x) for x in c.execute('SELECT * FROM imports ORDER BY id DESC LIMIT 10')]},ensure_ascii=False,indent=2))
 else:
  connect().close();print(f'Ozonchik: http://localhost:{a.port}');ThreadingHTTPServer(('0.0.0.0',a.port),Handler).serve_forever()
