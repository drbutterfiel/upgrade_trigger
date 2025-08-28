from collections import deque


class SlidingWindow:
    ''' Sliding window test class'''

    def __init__(self, sink, window_len=100): 
        # ensureSink(sink)
        self.window_length = window_len # ns
        self.hits = deque()
        self.window_start_time = 0
        self.sink = sink # initialize sink, then pass it as arg
        # all of these should implement interface that defines enque(hit)
        # sink & sliding window implement enque(hit) interface
        # SW should enforce seeing a monotonic time increase
        self.curr_time = None # minimum hit time initialized to track point in time of last hit seen
        self.prev_hit = None

        # sink = target (where data goes)

    def enque(self, myhit):
        # step 1: get hit time
        # hit wraps an I3RecoPulse
        # enforce monotonic time increase
        if self.curr_time != None and self.curr_time > myhit.resolveTime():
            raise RuntimeError(f'Out of order hit. last: {self.prev_hit.omkey} {self.prev_hit.resolveTime()}, current:  {myhit.omkey} {myhit.resolveTime()}, Delta t={self.prev_hit.resolveTime() - myhit.resolveTime()}')

        self.curr_time = myhit.resolveTime()

        # check to see if hits are within time window
        while len(self.hits) != 0 and self.curr_time - self.hits[0].resolveTime() > self.window_length:
            old_hit = self.hits.popleft()
            self.sink.enque(old_hit)

        self.hits.append(myhit)
        self.prev_hit = myhit

    def eos(self):
        ''' Call at end of data stream to flush all remaining hits to the sink'''
        while self.hits:
            self.sink.enque(self.hits.popleft())
        # print(f'SlidingWindow:eos:   {self.sink}')
        self.sink.eos()

        # could be another function but not rn
        # manage window function
        # need to look at window start time - current time < window length
        # probably will be a while loop to kick out hit and update current time until 
        # current time - start time is wihtin window

        # once window start time - current time >= window length
        # need to kick out next hit and update current time
        # kick out = give it to sink
        # sink.enque(kicked out hit)

        # yields window of hits
