from multiprocessing import Queue


class CustomQueue:
    TIMEOUT_FLAG = '[TIMEOUT]'
    END_FLAG = '[END]'

    def __init__(self, qsize: int):
        try:
            self.queue: Queue = Queue(maxsize=qsize)
        except:
            self.queue: Queue = Queue(maxsize=-1)
            print(f'CustomQueue, maxsize exceed {qsize} -> {self.queue._maxsize}')

    def get(self, timeout=60):
        try:
            sample = self.queue.get(timeout=timeout)
        except:
            sample = self.TIMEOUT_FLAG
        return sample

    def put(self, sample):
        self.queue.put(sample)

    def put_end_flag(self):
        self.queue.put(self.END_FLAG)

    def put_timeout_flag(self):
        self.queue.put(self.TIMEOUT_FLAG)

    def is_timeout(self, sample):
        return (type(sample) == str) and (sample == self.TIMEOUT_FLAG)

    def is_end_flag(self, sample):
        return (type(sample) == str) and (sample == self.END_FLAG)

    def close(self):
        self.queue.close()
        self.queue.join_thread()