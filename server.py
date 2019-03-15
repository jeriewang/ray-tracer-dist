from flask import Flask, request, render_template, current_app,g
from PIL import Image
import random
from itertools import filterfalse
import base64
from io import BytesIO
import datetime, json,time
import os,sys,atexit,collections
import sqlite3

WIDTH = 4096
HEIGHT = 4096
SAMPLE_RATE = 50000
MONITOR_REFRESH_INTERVAL = 1000 #max(1000, WIDTH * HEIGHT * 4 / 1024 / 1024 * 1000)  # assume 1MB/s network
# in miliseconds

os.chdir(os.path.dirname(os.path.abspath(__file__)))

app = Flask(__name__)


def get_db():
    if 'db' not in g:
        g.db = sqlite3.connect(
            'render.sqlite3'
        )
        g.db.row_factory = sqlite3.Row

    return g.db

@app.teardown_appcontext
def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.commit()
        db.close()

conn=sqlite3.connect('render.sqlite3')
c=conn.cursor()
c.executescript("""
CREATE TABLE IF NOT EXISTS pixels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                x INT NOT NULL,
                y INT NOT NULL,
                r INT,
                g INT,
                b INT,
                assigned BOOLEAN DEFAULT 0
        );
CREATE TABLE IF NOT EXISTS workers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(128) NOT NULL,
                worker_id INT NOT NULL,
                last_seen INTEGER,
                update_time FLOAT,
                is_terminated BOOLEAN DEFAULT 0
);
""")

c.execute("SELECT count() FROM pixels")
if c.fetchone()[0]==0:
    print("Initializing db")
    for y in range(HEIGHT):
        for x in range(WIDTH):
            c.execute('INSERT INTO pixels (x,y) VALUES (?,?)',(x,y))
    print('Done')

print('Cleaning up database...')
c.execute('UPDATE pixels SET assigned=0 WHERE r IS NULL;')
print('Loading pixels...')
c.execute('SELECT x,y,r,g,b FROM pixels WHERE r IS NOT NULL')
print('Initializing the image...')
image=Image.new('RGB',(WIDTH,HEIGHT))
for x,y,r,g_,b in c.fetchall():
   image.putpixel((x,y),(r,g_,b))
c.execute('SELECT count() FROM pixels WHERE r IS NOT NULL ')
total_rendered_pixels=c.fetchone()[0]
c.close()
conn.commit()


@app.route('/api/worker', methods=['PUT'])
def worker_join():
    global worker_status
    
    data=request.get_json()
    name=data['name']
    id=data['id']
    c=get_db().cursor()
    c.execute('SELECT * FROM workers WHERE name=? AND worker_id=?',(name,id))

    if not c.fetchone():
        c.execute('INSERT INTO workers (name, worker_id, last_seen) VALUES (?,?,?)',(name, id, time.time()))
    close_db()
    
    return ''

@app.route('/api/worker', methods=['GET'])
def task_retrieval():
    c=get_db().cursor()
    c.execute('SELECT id,x,y FROM pixels WHERE assigned=0 LIMIT 1')
    r=c.fetchone()
    if r is not None:
        id,x,y=r
    else:
        return 'COMPLETED'
    c.execute('UPDATE pixels SET assigned=1 WHERE id=?',(id,))
    c.close()
    close_db()
    
    return '{} {} {} {} {}'.format(
            x,
            y,
            SAMPLE_RATE,
            WIDTH,
            HEIGHT
    )


@app.route('/api/worker', methods=['POST'])
def task_complete():
    global total_rendered_pixels
    data = request.get_json()
    task = data['task'].split()
    x = int(task[0])
    y = int(task[1])
    pixel = tuple(map(int, data['pixel'].split()))
    image.putpixel((x, y), pixel)
    c=get_db().cursor()
    c.execute('UPDATE pixels SET r=?,g=?,b=? WHERE id=?',(*pixel,y*4096+x))
    c.execute('SELECT last_seen FROM workers WHERE name=? AND worker_id=?',(data['name'],int(data['id'])))
    t=time.time()-c.fetchone()[0]
    c.execute('UPDATE workers SET last_seen=?, update_time=?, is_terminated=0 WHERE name=? AND worker_id=?',(time.time(),t,data['name'],data['id']))
    c.close()
    get_db().commit()
    close_db()
    total_rendered_pixels+=1
    return ''


@app.route('/api/worker', methods=['DELETE'])
def worker_leave():
    data = request.get_json()
    c=get_db().cursor()
    c.execute('UPDATE workers SET is_terminated=1 WHERE name=? and worker_id=?',(data['name'],data['id']))
    close_db()
    return ''


@app.route('/api/live-image', methods=['GET'])
def live_image():
    w = request.args.get('w', None)
    h = request.args.get('h', None)
    if w=='undefined':
        return ''
    buffer = BytesIO()
    if w is None and h is None:
        image.save(buffer, format="BMP")
    else:
        thumb = image.copy()
        thumb.thumbnail((max(int(float(w)), 1), max(int(float(h)), 1)))
        thumb.save(buffer, format="BMP")
    img_str = base64.b64encode(buffer.getvalue()).decode()
    return img_str


@app.route('/api/worker-status', methods=['GET'])
def get_worker_status():
    c=get_db().cursor()
    c.execute('SELECT name, worker_id, id, last_seen, update_time FROM workers WHERE is_terminated=0 ORDER BY name, id')
    hosts={}
    prev_name=None
    t=time.time()
    try:
        for res in c.fetchall():
            if t-res['last_seen']>120:
                c.execute('UPDATE workers SET is_terminated=1 WHERE id=?',(res['id'],))
            if res['name']!=prev_name:
                hosts[res['name']]={'name':res['name'],'workers':[{'worker_id':res['worker_id'],'id':res['id'],'last_seen':res['last_seen'],'time':round(res['update_time'],3)}]}
                prev_name=res['name']
            else:
                hosts[prev_name]['workers'].append({'worker_id':res['worker_id'],'id':res['id'],'last_seen':res['last_seen'],'time':round(res['update_time'],3)})
        
        c.execute('SELECT count() as c, avg(update_time) as avg FROM workers WHERE is_terminated=0')
        res=c.fetchone()
        return json.dumps({
            'avg_pixel_time':round(res['avg'],3),
            'active_workers':res['c'],
            'hosts':hosts,
            'ETA':str(datetime.timedelta(seconds=(WIDTH*HEIGHT-total_rendered_pixels)/res['c']*res['avg']))
        })
    except (NameError,KeyError, TypeError): #res is not defined. No active workers
        return json.dumps({
            'active_workers':0,
            'avg_pixel_time':0,
            'ETA':0,
            'hosts':{}
        })



@app.route('/live-view', methods=['GET'])
def live_view():
    s = render_template('live-view.jinja2', request=request, refresh_frequency=MONITOR_REFRESH_INTERVAL)
    return s

if __name__ == '__main__':
    app.run()
