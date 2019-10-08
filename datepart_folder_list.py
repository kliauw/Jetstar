from datetime import date, datetime, timedelta

def perdelta(start, end, delta):
    curr = start
    while curr < end:
        yield curr
        curr += delta

>>> for result in perdelta(date(2011, 10, 10), date(2011, 12, 12), timedelta(days=4)):
...     print result
