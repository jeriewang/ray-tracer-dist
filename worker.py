from __future__ import print_function, absolute_import

from subprocess import Popen, PIPE
import os, sys, multiprocessing, time, urllib2, signal, socket, logging, traceback

os.chdir(os.path.dirname(os.path.abspath(__file__)))

endpoint = 'http://134.209.223.110/api/worker'

process_count = int(input('Process count?: '))
detach = raw_input('Detach process? y/n: ').lower().strip().startswith('y')


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


def start_worker(i):
    logging.basicConfig(filename='%d.log' % i, filemode='w', level=logging.DEBUG)
    
    name = socket.gethostname()
    req = PutRequest(endpoint, data='{"name":"%s","id":"%d"}' % (name, i))
    urllib2.urlopen(req)
    
    logging.info('Worker %d started. PID %d' % (i,multiprocessing.current_process().pid))
    
    def handler(signum, frame):
        try:
            name = socket.gethostname()
            req = DeleteRequest(endpoint, data='{"name":"%s","id":"%d"}' % (name, i))
            urllib2.urlopen(req)
        finally:
            logger.info('Exiting due to signal %d' % signum)
            os.kill(multiprocessing.current_process().pid, signal.SIGKILL)
    
    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGQUIT, handler)
    signal.signal(signal.SIGABRT, handler)
    signal.signal(signal.SIGSEGV, handler)
    signal.signal(signal.SIGILL, handler)
    
    print('Started worker %d: %d' % (i, multiprocessing.current_process().pid))
    logger = logging.getLogger(str(i))
    while True:
        try:
            task = urllib2.urlopen(endpoint).read()  # GET
            logging.info('Task %s retrieved' % task)
            if task == 'COMPLETED':
                logger.info('Task completed. Exiting...')
                os._exit(0)
            sample_rate, x, y, width, height = map(int, task.split())
            proc = Popen('./Ray_Tracer {}'.format(task), universal_newlines=True, shell=True, stdin=PIPE,
                         stdout=PIPE)
            pixel, _ = proc.communicate()
            logging.info('Pixel rendered. Value: %s, exit code: %d' % (pixel, proc.returncode))
            req = urllib2.Request(endpoint, data='{{"pixel":"{}","task":"{}","name":"{}","id":"{}"}}'.format(pixel, task, name, i))
            req.add_header('Content-Type', 'application/json')
            urllib2.urlopen(req)
        except:
            logging.error(traceback.format_exc())


if detach:
    if os.fork() != 0:
        sys.exit(0)
for i in range(process_count):
    multiprocessing.Process(target=start_worker, args=(i,)).start()
