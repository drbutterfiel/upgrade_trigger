from uglc.slidingwindow import SlidingWindow

class MMLC:
    ''' Only marks the hits, does not release hits or move the window'''
    
    class MMLCConfig:
        def __init__(self, window_len=100):
            self.window_length=window_len

    def __init__(self, string, config, sink):
        self.string = string
        self.sw = SlidingWindow(sink, config.window_length)

    def enque(self, hit):
        # receives / consumes a time ordered stream of hits from a string
        self.sw.enque(hit)
        self.examine(self.sw.hits) # run SMLC algorithm to flag hits

    def examine(self, window_hits): # PRIORITY #
        # TODO: Implement actual MMLC logic here.
        pass

    def eos(self):
        ''' Call at end of data stream to flush all remaining hits to the sink'''
        # print(f'MMLC:eos: [{self.string}]')
        self.sw.eos()
