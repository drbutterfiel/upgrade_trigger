
from uglc.slidingwindow import SlidingWindow
from pipeline.injest import Geometry

class SMLC:
    ''' Only marks the hits, does not release hits or move the window'''
    
    class SMLCConfig:
        def __init__(self, window_len, multiplicity):
            self.window_len = window_len
            self.multiplicity = multiplicity

        def lookup(device_type):
            match device_type:
                case Geometry.DeviceType.DEGG:
                    return SMLC.SMLCConfig(250, 4)
                case Geometry.DeviceType.MDOM:
                    return SMLC.SMLCConfig(100, 2)
                case _:
                    raise RuntimeError(f'Unsupported device: {device_type}');


    def __init__(self, modulekey, config, sink):
        self.modulekey = modulekey
        self.config = config
        self.sw = SlidingWindow(sink, config.window_len)

    def enque(self, hit):
        # receives / consumes a time ordered stream of hits from a single module
        self.sw.enque(hit)
        self.examine(self.sw.hits) # run SMLC algorithm to flag hits

    def examine(self, window_hits): # PRIORITY #
        # TODO: Implement actual SMLC logic here.
        self.multiplicity_algo(self.config.multiplicity, window_hits)

    def eos(self):
        ''' Call at end of data stream to flush all remaining hits to the sink'''
        # print(f'SMLC:eos: [{self.modulekey.string}-{self.modulekey.om}]')
        self.sw.eos()


    def multiplicity_algo(self, multiplicity, window_hits):
        if len(window_hits) >= multiplicity:
            for hit in window_hits:
                hit.markSMLC()        
