from __future__ import print_function, absolute_import

from subprocess import Popen, PIPE
import os, sys, multiprocessing, time, urllib2, signal,socket

os.chdir(os.path.dirname(os.path.abspath(__file__)))

endpoint = 'http://localhost:5000/api/worker'

process_count=int(input('Process count?: '))

class PutRequest(urllib2.Request):
    def __init__(self, *args, **kwargs):
        
        urllib2.Request.__init__(self, *args, **kwargs)
        self.add_header('Content-Type', 'application/json')
        
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
        try:
            name = socket.gethostname()
            req=DeleteRequest(endpoint,data='{"name":"%s","id":"%d"}'%(name,worker_id))
            urllib2.urlopen(req)
            
        finally:
            print('Terminating worker', worker_id)
            os.kill(worker_pid,signal.SIGKILL)
    return handler

def start_worker(i):
    name=socket.gethostname()
    req = PutRequest(endpoint,data='{"name":"%s","id":"%d"}'%(name,i))
    urllib2.urlopen(req)
    
    signal.signal(signal.SIGINT,terminate_worker(i,multiprocessing.current_process().pid))
    signal.signal(signal.SIGTERM,terminate_worker(i,multiprocessing.current_process().pid))
    signal.signal(signal.SIGQUIT,terminate_worker(i,multiprocessing.current_process().pid))
    signal.signal(signal.SIGABRT,terminate_worker(i,multiprocessing.current_process().pid))
    signal.signal(signal.SIGSEGV, terminate_worker(i, multiprocessing.current_process().pid))
    signal.signal(signal.SIGILL, terminate_worker(i, multiprocessing.current_process().pid))
    
    print('Started worker %d: %d' % (i,multiprocessing.current_process().pid))
    while True:
        try:
            task=urllib2.urlopen(endpoint).read() #GET
            if task=='COMPLETED':
                print('Task completed')
                os._exit(0)
            sample_rate,x,y,width,height=map(int,task.split())
            proc = Popen('./Ray_Tracer {}'.format(task), universal_newlines=True, shell=True, stdin=PIPE,
                         stdout=PIPE)
            pixel, _ = proc.communicate()
            req=urllib2.Request(endpoint,data='{{"pixel":"{}","task":"{}","name":"{}","id":"{}"}}'.format(pixel,task,name,i))
            req.add_header('Content-Type', 'application/json')
            urllib2.urlopen(req)
        except:
            pass

for i in range(process_count):
    multiprocessing.Process(target=start_worker,args=(i,)).start()
