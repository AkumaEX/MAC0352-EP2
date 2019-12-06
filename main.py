from worker import worker
from leader import leader
from first import first
import sys
import threading


def worker_t(debug):
    wo = worker(debug)
    wo.run()


def leader_t(debug):
    lo = leader(debug)
    lo.run()


def first_t(filename, debug):
    fo = first(filename, debug)
    fo.run()


# primeiro computador sem debug ou qualquer computador com debug
if len(sys.argv) > 1:
    if sys.argv[1].lower() == 'debug':  # qualquer computador com debug
        w_t = threading.Thread(target=worker_t, args=(True,))
        w_t.start()
    elif len(sys.argv) > 2:  # primeiro computador com debug
        f_t = threading.Thread(target=first_t, args=(sys.argv[1], True))
        f_t.start()
        l_t = threading.Thread(target=leader_t, args=(True,))
        l_t.start()
        w_t = threading.Thread(target=worker_t, args=(True,))
        w_t.start()
    else:  # primeiro computador sem debug
        f_t = threading.Thread(target=first_t, args=(sys.argv[1], False))
        f_t.start()
        l_t = threading.Thread(target=leader_t, args=(False,))
        l_t.start()
        w_t = threading.Thread(target=worker_t, args=(False,))
        w_t.start()
else:   # qualquer computador exceto o primeiro sem debug
    w_t = threading.Thread(target=worker_t, args=(False,))
    w_t.start()
