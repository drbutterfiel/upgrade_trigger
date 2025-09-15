from collections import deque

class MMLC:
    ''' Synthesizes MMLC for hits.
        based on a a sliding window of windows with mmlc calculated hit-by-hit in time order


        Synopsis:

            maintains a buffer of hits with associated LC window in time-order

            -----------------------------------------------------
            |       held        |    pending                    |
            -----------------------------------------------------
            | x | x | x | x | x | x | x | x | x | x | x | x | x |
            -----------------------------------------------------
                                  |                       |   |
                                  |                       |   |
                                  |                       |    pit
                                  |                       |
                                  |              |----------------|
                                  |                   <window>
                           |-------------|  
                               <window>
                          back          fwd      








            at each hit insertion:

                  while pending.head.fwd < pit
                        calulate mmlc on pending.head (using all relavent hits in held+pending)
                        promote pending.head to held

                  while held.head.time < pit + MAX_WINDOW
                        release held.head to sink


    '''
    


    class MMLCConfig:
        def __init__(self, window_len=100):
            self.MAX_WINDOW=100;
            self.window_length=window_len

    class MMLCWindow:
        def __init__(self, hit, t_back, t_fwd):
            self.hit = hit
            self.t_hit = hit.resolveTime()
            self.t_start = self.t_hit - t_back  #todo do we bound to 0?
            self.t_end = self.t_hit + t_fwd
            self.cost = 0
            self.mmlc = 0

        def count(self, hit):
            # todo this is not the mmlc alg, need to check id neighbor
            t= hit.resolveTime()
            self.cost += 1
            if t >= self.t_start and t<= self.t_end:
                self.mmlc +=1



    def __init__(self, string, config, sink):
        self.string = string
        self.pending = deque()
        self.held = deque()
        self.MAX_WINDOW = config.MAX_WINDOW
        self.fwd = 50
        self.back = 50
        self.sink = sink


    def enque(self, hit):

        window = MMLC.MMLCWindow(hit, self.back, self.fwd)

        self.pending.append(window)

        self.examine(window.t_hit)

        self.release(self.pending[0].t_hit - self.MAX_WINDOW)


    def examine(self, pit):

        # present all hits to pending hit windows
        # that are ready
        while self.pending and self.pending[0].t_end < pit:
            processing = self.pending.popleft()
            for hw in self.held:
                processing.count(hw.hit)
            for hw in self.pending:
                processing.count(hw.hit)
            self.held.append(processing)


    def release(self, pit):
        while self.held:
            if self.held[0].t_hit < pit:
                hw = self.held.popleft()
                # if hw.cost>4:
                #     print(f'Release MMLC {hw.hit.omkey} @ {hw.t_hit}, cost: {hw.cost}, mmlc {hw.mmlc}')
                if hw.mmlc > 0:
                    hw.hit.markMMLC()
                self.sink.enque(hw.hit)
            else:
                break;

        


    def eos(self):
        ''' Call at end of data stream to flush all remaining hits to the sink'''
        # print(f'MMLC:eos: [{self.string}]')
        self.examine(float('inf'))
        self.release(float('inf'))
        self.sink.eos()
