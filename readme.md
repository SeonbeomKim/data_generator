# example

```python
from src.AbstractDataGenerator import AbstractDataGenerator

if __name__ == '__main__':
    class DataGenerator(AbstractDataGenerator):
        def preprocess(self, sample: int) -> int:
            return sample + 1

    data = range(10)

    data_generator = DataGenerator(data=data, n_process=3, qsize=100000000, repeat=False, verbose=True)

    for each in data_generator:
        print(each)

   
'''
CustomQueue, maxsize exceed 100000000 -> 32767
CustomQueue, maxsize exceed 300000000 -> 32767
  (Producer)create process(pid: 9268)
  (DataGenerator)create process(pid: 9269)
  (DataGenerator)create process(pid: 9270)
  (DataGenerator)create process(pid: 9271)
1
2
3
4
5
6
7
8
9
10
  (DataGenerator)terminate process(pid: 9271)
  (DataGenerator)terminate process(pid: 9270)
  (DataGenerator)terminate process(pid: 9269)
  (Producer)terminate process(pid: 9268)
'''

```