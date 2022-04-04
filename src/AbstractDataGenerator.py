import random
from abc import abstractmethod, ABCMeta
from multiprocessing import Process
from typing import Any, Iterable, List

import numpy as np

from src.Queue import CustomQueue


class AbstractDataGenerator(metaclass=ABCMeta):
    def __init__(self, data: Iterable, n_process: int, qsize: int, repeat: bool, shuffle: bool):
        self.data = data
        self.n_process = n_process
        self.repeat = repeat
        self.shuffle = shuffle
        self.out_queue: CustomQueue = CustomQueue(qsize=qsize)
        self.in_queue: CustomQueue = CustomQueue(qsize=qsize * n_process)
        self.processors: List[Process] = []

    @abstractmethod
    def preprocess(self, sample: Any) -> Any:
        raise NotImplementedError

    def __enter__(self):
        self.producer = Producer(queue=self.in_queue, repeat=self.repeat, shuffle=self.shuffle)
        self.producer.run(data=self.data)
        self.run()

    def __exit__(self, type, value, tb):
        self.terminate()
        self.producer.terminate()

    def __call__(self):  # for tf.data.Dataset.from_generator
        with self:
            while True:
                sample = self.out_queue.get()
                if not sample:
                    continue

                if self.out_queue.is_timeout(sample):
                    print('timeout')
                    break

                if self.out_queue.is_end_flag(sample):
                    break

                yield sample

    def __iter__(self):
        return self.__call__()

    def run(self):
        if self.processors:
            self.terminate()

        for i in range(self.n_process):
            random_seed = np.random.randint(int(1e+5))
            process = Process(target=self.fill_queue, args=(self.in_queue, self.out_queue, random_seed,))
            process.daemon = True
            process.start()
            print('  (%s)create process(pid: %d)' % (self.__class__.__name__, process.pid))

            self.processors.append(process)

    def terminate(self):
        self.out_queue.close()

        while self.processors:
            process = self.processors.pop(-1)
            print('  (%s)terminate process(pid: %d)' % (self.__class__.__name__, process.pid))

            process.join(timeout=0.5)
            if process.is_alive():
                process.terminate()
                process.join()

    def fill_queue(self, in_queue: CustomQueue, out_queue: CustomQueue, random_seed: int):
        random.seed(random_seed)
        np.random.seed(random_seed)

        try:
            while True:
                sample = in_queue.get()
                if in_queue.is_timeout(sample):
                    out_queue.put_timeout_flag()
                    break
                if in_queue.is_end_flag(sample):
                    out_queue.put_end_flag()
                    break

                sample = self.preprocess(sample=sample)
                out_queue.put(sample)
            out_queue.put_end_flag()
        except:  # out_queue.close가 call될 때 에러
            pass


class Producer:
    def __init__(self, queue: CustomQueue, repeat: bool, shuffle: bool):
        self.queue = queue
        self.repeat = repeat
        self.shuffle = shuffle
        self.process = None

    def run(self, data: Iterable):
        self.process = Process(target=self.fill_queue, args=(data,))
        self.process.daemon = True
        self.process.start()

        print('  (%s)create process(pid: %d)' % (self.__class__.__name__, self.process.pid))

    def fill_queue(self, data: Iterable):
        try:
            while True:
                try:
                    if self.shuffle:
                        random.shuffle(data)

                    for sample in data:
                        if not sample:
                            continue
                        self.queue.put(sample)

                    if not self.repeat:
                        break

                except Exception as e:
                    print("Producer Error", e)
                    break

            self.queue.put_end_flag()
        except:  # queue.close가 call될 때 에러
            pass

    def terminate(self):
        assert self.process is not None

        self.queue.close()
        process = self.process
        print('  (%s)terminate process(pid: %d)' % (self.__class__.__name__, process.pid))

        process.join(timeout=0.5)
        if process.is_alive():
            process.terminate()
            process.join()


if __name__ == '__main__':
    class DataGenerator(AbstractDataGenerator):
        def preprocess(self, sample: int) -> int:
            return sample + 1

    data = list(range(10))

    data_generator = DataGenerator(data=data, n_process=3, qsize=100000000, repeat=False, shuffle=True)

    for each in data_generator:
        print(each)
