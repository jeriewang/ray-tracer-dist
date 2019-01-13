from __future__ import print_function, absolute_import

from subprocess import Popen, PIPE
import os, sys, multiprocessing, time, urllib2, signal

os.chdir(os.path.dirname(os.path.abspath(__file__)))

endpoint = 'http://localhost:5000/api/ray_tracer'

process_count=int(input('Process count?: '))

class PutRequest(urllib2.Request):
    def __init__(self, *args, **kwargs):
        urllib2.Request.__init__(self, *args, **kwargs)
    
    def get_method(self):
        return 'PUT'


class DeleteRequest(urllib2.Request):
    def __init__(self, *args, **kwargs):
        urllib2.Request.__init__(self, *args, **kwargs)
        self.add_header('Content-Type', 'application/json')
    
    def get_method(self):
        return 'DELETE'

def terminate_worker(worker_id,worker_pid):
    def handler(signum, frame):
        req=DeleteRequest(endpoint,data='{"worker_id":"%d"}'%worker_id)
        urllib2.urlopen(req)
        print('Terminating worker',worker_id)
        os.kill(worker_pid,signal.SIGKILL)
    return handler

def start_worker():
    req = PutRequest(endpoint)
    worker_id = int(urllib2.urlopen(req).read())
    
    signal.signal(signal.SIGINT,terminate_worker(worker_id,multiprocessing.current_process().pid))
    signal.signal(signal.SIGTERM,terminate_worker(worker_id,multiprocessing.current_process().pid))
    signal.signal(signal.SIGQUIT,terminate_worker(worker_id,multiprocessing.current_process().pid))
    signal.signal(signal.SIGABRT,terminate_worker(worker_id,multiprocessing.current_process().pid))
    
    print('Started worker %d' % worker_id)
    while True:
        task=urllib2.urlopen(endpoint).read() #GET
        
        sample_rate,x,y,width,height=map(int,task.split())
        proc = Popen('./Ray_Tracer {}'.format(task), universal_newlines=True, shell=True, stdin=PIPE,
                     stdout=PIPE)
        pixel, _ = proc.communicate()
        req=urllib2.Request(endpoint,data='{{"pixel":"{}","task":"{}","worker_id":"{}"}}'.format(pixel,task,worker_id))
        req.add_header('Content-Type', 'application/json')
        urllib2.urlopen(req)

for i in range(process_count):
    multiprocessing.Process(target=start_worker).start()
