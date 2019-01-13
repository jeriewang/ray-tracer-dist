from flask import Flask, request, render_template_string
from PIL import Image
import random
from itertools import filterfalse
import base64
from io import BytesIO
from collections import defaultdict
import datetime, json

WIDTH = 4096
HEIGHT = 4096
SAMPLE_RATE = 1
MONITOR_REFRESH_INTERVAL = max(1000, WIDTH * HEIGHT * 4 / 1024 / 1024 * 1000) # assume 1MB/s network

worker_count = 0

image = Image.new('RGB', (WIDTH, HEIGHT))

completed_tasks = [(i, j, False) for i in range(WIDTH) for j in range(HEIGHT)]
# TODO: use a hash map

worker_status = {}
# TODO: sort worker status by MAC
# TODO: auto-remove terminated computers from web display. Send an email instead.

app = Flask(__name__)


@app.route('/api/ray_tracer', methods=['PUT'])
def worker_join():
    global worker_count
    worker_count += 1
    
    worker_status[worker_count] = {
        'status'    : 'alive',
        'heartbeats': [datetime.datetime.now()]
    }
    
    return str(worker_count)


@app.route('/api/ray_tracer', methods=['GET'])
def task_retrieval():
    global worker_count
    
    pixel = random.choice(list(filterfalse(lambda t: t[2], completed_tasks)))
    return '{} {} {} {} {}'.format(
            pixel[0],
            pixel[1],
            SAMPLE_RATE,
            WIDTH,
            HEIGHT
    )


@app.route('/api/ray_tracer', methods=['POST'])
def task_complete():
    data = request.get_json()
    task = data['task'].split()
    x = int(task[0])
    y = int(task[1])
    pixel = tuple(map(int, data['pixel'].split()))
    image.putpixel((x, y), pixel)
    completed_tasks[y + x * y] = (x, y, True)
    worker_status[int(data['worker_id'])]['heartbeats'].append(datetime.datetime.now())
    return ''


@app.route('/api/ray_tracer', methods=['DELETE'])
def worker_leave():
    worker_id = int(request.get_json()['worker_id'])
    worker_status[worker_id]['status'] = 'terminated'
    return ''


@app.route('/api/live-image', methods=['GET'])
def live_image():
    w=request.args.get('w',None)
    h=request.args.get('h',None)
    buffer = BytesIO()
    if w is None and h is None:
        image.save(buffer, format="BMP")
    else:
        thumb=image.copy()
        thumb.thumbnail((max(int(float(w)),1),max(int(float(h)),1)))
        thumb.save(buffer, format="BMP")
    img_str = base64.b64encode(buffer.getvalue()).decode()
    return img_str


@app.route('/api/worker-status', methods=['GET'])
def get_worker_status():
    res = {}
    for k, v in worker_status.items():
        d = {}
        d['status'] = v['status']
        heartbeats = v['heartbeats']
        d['last_seen'] = round((datetime.datetime.now() - heartbeats[-1]).total_seconds(), 2)
        diff = 0
        for i in range(1, len(heartbeats)):
            diff += heartbeats[i].timestamp() - heartbeats[i - 1].timestamp()
        
        d['average_update_frequency'] = round(diff / len(heartbeats), 2)
        res[k] = d
    
    return json.dumps(res)


@app.route('/live-view', methods=['GET'])
def live_view():
    f = open('live-view.jinja2')
    s = render_template_string(f.read(), request=request,refresh_frequency=MONITOR_REFRESH_INTERVAL)
    f.close()
    return s
