
from uglc.slidingwindow import SlidingWindow

class SMLC:
    ''' Only marks the hits, does not release hits or move the window'''
    
    class SMLCConfig:
        def __init__(self, window_len=100):
            self.window_length=window_len

    def __init__(self, modulekey, config, sink):
        self.modulekey = modulekey
        self.sw = SlidingWindow(sink, config.window_length)

    def enque(self, hit):
        # receives / consumes a time ordered stream of hits from a single module
        self.sw.enque(hit)
        self.examine(self.sw.hits) # run SMLC algorithm to flag hits

    def examine(self, window_hits): # PRIORITY #
        # TODO: Implement actual SMLC logic here.
        self.multiplicity_algo(4, window_hits)

    def eos(self):
        ''' Call at end of data stream to flush all remaining hits to the sink'''
        # print(f'SMLC:eos: [{self.modulekey.string}-{self.modulekey.om}]')
        self.sw.eos()


    def multiplicity_algo(self, multiplicity, window_hits):
        if len(window_hits) > multiplicity:
            for hit in window_hits:
                hit.markSMLC()        
