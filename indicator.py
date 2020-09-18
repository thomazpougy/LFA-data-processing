import sys
import time
import threading


class Running:
    busy = False
    delay = 0.1

    @staticmethod
    def cursor_running():
        while 1:
            for cursor in '|/-\\':
                yield cursor

    def __init__(self, delay=None):
        self.cursor_generate = self.cursor_running()
        if delay and float(delay): self.delay = delay

    def cursor_task(self):
        while self.busy:
            sys.stdout.write(next(self.cursor_generate))
            sys.stdout.flush()
            time.sleep(self.delay)
            sys.stdout.write('\b')
            sys.stdout.flush()

    def __enter__(self):
        self.busy = True
        threading.Thread(target=self.cursor_task).start()

    def __exit__(self, exception, value, tb):
        self.busy = False
        time.sleep(self.delay)
        if exception is not None:
            return False
