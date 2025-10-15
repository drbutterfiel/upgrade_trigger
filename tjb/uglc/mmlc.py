from collections import deque
from pipeline.injest import Geometry

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
    


    # hardcoded
    class MMLCConfig:

        class ModuleConfig:
            def __init__(self, t_back, t_fwd, span_up, span_down, multiplicity):
                    self.t_back =t_back
                    self.t_fwd = t_fwd
                    self.span_up = span_up
                    self.span_down = span_down
                    self.multiplicity = multiplicity

        def __init__(self):
            self.MAX_WINDOW=250; #must be >= the longest module window
            self.degg_cfg = MMLC.MMLCConfig.ModuleConfig(250, 250, 8, 8, 2)
            self.mdom_cfg = MMLC.MMLCConfig.ModuleConfig(125, 125, 4, 4, 2)


    class MMLCWindow:
        def __init__(self, hit, t_back, t_fwd, span_up, span_down, multiplicity):
            self.hit = hit
            self.t_hit = hit.resolveTime()
            self.t_start = self.t_hit - t_back  #todo do we bound to 0?
            self.t_end = self.t_hit + t_fwd
            self.span_up = span_up
            self.span_down = span_down
            self.multiplicity = multiplicity
            self.cost = 0
            self.mmlc = 0

        def count(self, hit):
            # todo this is not the mmlc alg, need to check id neighbor
            t= hit.resolveTime()
            self.cost += 1
            if hit.omkey.om == self.hit.omkey.om:
                return
            if t >= self.t_start and t<= self.t_end:
                if hit.omkey.om <= (self.hit.omkey.om + self.span_down) or hit.omkey.om >= (self.hit.omkey.om - self.span_up):
                    self.mmlc +=1
                    if self.mmlc >= self.multiplicity:
                        self.hit.markMMLC()



    def __init__(self, string, config, sink):
        self.string = string
        self.config = config
        self.pending = deque()
        self.held = deque()
        self.sink = sink


    def enque(self, hit):

        
        match hit.device_type:
            case Geometry.DeviceType.DEGG:
                cfg = self.config.degg_cfg
            case Geometry.DeviceType.MDOM:
                cfg = self.config.mdom_cfg
            case _:
                raise RuntimeError(f'Unsupported device: {device_type}');


        window = MMLC.MMLCWindow(hit, cfg.t_back, cfg.t_fwd, cfg.span_up, cfg.span_down, cfg.multiplicity)
        self.pending.append(window)

        self.examine(window.t_hit)

        self.release(self.pending[0].t_hit - self.config.MAX_WINDOW)


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
                # if hw.mmlc > 0:
                #     hw.hit.markMMLC()
                
                self.sink.enque(hw.hit)
            else:
                break;

        


    def eos(self):
        ''' Call at end of data stream to flush all remaining hits to the sink'''
        # print(f'MMLC:eos: [{self.string}]')
        self.examine(float('inf'))
        self.release(float('inf'))
        self.sink.eos()
