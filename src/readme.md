# example

```python
from src.AbstractDataGenerator import AbstractDataGenerator

class DataGenerator(AbstractDataGenerator):
    def preprocess(self, sample: int) -> int:
        return sample + 1
        
        
data = range(10)
      
data_generator = DataGenerator(data=data, n_process=3, qsize=1000, repeat=False)

for each in data_generator:
    print(each)
   
'''
  (DataGenerator)create process(pid: 11139)
  (DataGenerator)create process(pid: 11141)
  (DataGenerator)create process(pid: 11142)
2
3
4
5
6
7
8
9
10
  (DataGenerator)terminate process(pid: 11142)
  (DataGenerator)terminate process(pid: 11141)
  (DataGenerator)terminate process(pid: 11139)
'''

```