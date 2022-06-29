import random
from abc import abstractmethod, ABCMeta
from multiprocessing import Process
from typing import Any, Iterable, List, Union

import numpy as np

from src.Queue import CustomQueue


def gen():
    for i in range(3):
        yield i


generator_type = type(gen())


class AbstractDataGenerator(metaclass=ABCMeta):
    def __init__(self, data: Iterable, n_process: int, qsize: int, repeat: bool, verbose: bool = False):
        self.data: Iterable = data
        self.n_process: int = n_process
        self.repeat: bool = repeat
        self.qsize = qsize
        self.producer = Producer(qsize=self.qsize, repeat=self.repeat, verbose=verbose)
        self.out_queue: Union[CustomQueue, None] = None
        self.processors: List[Process] = []
        self.verbose = verbose

    @abstractmethod
    def preprocess(self, sample: Any) -> Any:
        raise NotImplementedError

    def __enter__(self):
        self.run()

    def __exit__(self, type, value, tb):
        self.terminate()

    def __call__(self):  # for tf.data.Dataset.from_generator
        meet_end_flag: bool = False
        with self:
            while True:
                sample = self.out_queue.get()
                if not sample:
                    continue

                if self.out_queue.is_timeout(sample):
                    if self.verbose:
                        print('timeout (get_next in out_queue)')
                    break

                if self.out_queue.is_end_flag(sample):
                    meet_end_flag = True
                    break

                yield sample

            # 남아있는 것 처리 (multi proc이라 남은게 있을 수 있음)
            if meet_end_flag == True:
                while True:
                    sample = self.out_queue.get(timeout=1.0)
                    if self.out_queue.is_timeout(sample):
                        break

                    yield sample

    def run(self):
        if self.processors:
            self.terminate()

        self.producer.run(data=self.data)

        self.out_queue: CustomQueue = CustomQueue(qsize=self.qsize)
        for i in range(self.n_process):
            random_seed = np.random.randint(int(1e+5))
            process = Process(target=self.fill_queue, args=(self.producer.in_queue, self.out_queue, random_seed,))
            process.daemon = True
            process.start()
            if self.verbose:
                print('  (%s)create process(pid: %d)' % (self.__class__.__name__, process.pid))

            self.processors.append(process)

    def terminate(self):
        self.out_queue.close()
        while self.processors:
            process = self.processors.pop(-1)
            if self.verbose:
                print('  (%s)terminate process(pid: %d)' % (self.__class__.__name__, process.pid))

            process.join(timeout=0.5)
            if process.is_alive():
                process.terminate()
                process.join()

        self.producer.terminate()

    def __iter__(self):
        return self()

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
                if type(sample) == generator_type:
                    for each in sample:
                        out_queue.put(each)
                else:
                    out_queue.put(sample)
        except:  # out_queue.close가 call될 때 에러
            pass


class Producer:
    def __init__(self, qsize: int, repeat: bool, verbose: bool = False):
        self.qsize: int = qsize
        self.repeat: bool = repeat
        self.process: Union[Process, None] = None
        self._in_queue: Union[CustomQueue, None] = None
        self.verbose: bool = verbose

    def run(self, data: Iterable):
        self._in_queue: CustomQueue = CustomQueue(qsize=self.qsize)
        self.process = Process(target=self.fill_queue, args=(data,))
        self.process.daemon = True
        self.process.start()
        if self.verbose:
            print('  (%s)create process(pid: %d)' % (self.__class__.__name__, self.process.pid))

    def terminate(self):
        assert self.process is not None

        self._in_queue.close()
        process = self.process
        if self.verbose:
            print('  (%s)terminate process(pid: %d)' % (self.__class__.__name__, process.pid))

        process.join(timeout=0.5)
        if process.is_alive():
            process.terminate()
            process.join()

    def fill_queue(self, data: Iterable):
        try:
            while True:
                try:
                    for sample in data:
                        if sample is None:
                            continue
                        self._in_queue.put(sample)

                    if not self.repeat:
                        break

                except Exception as e:
                    print("Producer Error", e)
                    break

            self._in_queue.put_end_flag()
        except:  # queue.close가 call될 때 에러
            pass

    @property
    def in_queue(self) -> Union[None, CustomQueue]:
        return self._in_queue


if __name__ == '__main__':
    class DataGenerator(AbstractDataGenerator):
        def preprocess(self, sample: int) -> int:
            return sample + 1


    class DataGenerator2(AbstractDataGenerator):
        def preprocess(self, sample: int) -> int:
            for i in range(sample):
                yield sample, i


    data = range(100)
    data_generator = DataGenerator(data=data, n_process=3, qsize=1000000, repeat=False)
    for each in data_generator:
        print(each)
    for each in data_generator:
        print(each)

    data_generator2 = DataGenerator2(data=data, n_process=3, qsize=1000000, repeat=False)
    for each in data_generator2:
        print(each)
    for each in data_generator2:
        print(each)
