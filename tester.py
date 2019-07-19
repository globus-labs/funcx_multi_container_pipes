
from dataclasses import dataclass, field
from typing import Any

from queue import PriorityQueue


@dataclass(order=True)
class PrioritizedItem:
    priority: int
    item: Any = field(compare=False)



q = PriorityQueue()

q.put(PrioritizedItem(4, 'hello1'))
q.put(PrioritizedItem(1, 'hello2'))
q.put(PrioritizedItem(2, 'hello3'))

print(q.get().item)