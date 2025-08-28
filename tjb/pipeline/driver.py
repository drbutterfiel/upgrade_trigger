from collections import deque

from pipeline.pipeline import Stopwatch
from pipeline.pipeline import Counter
from pipeline.pipeline import Pipeline
from pipeline.injest import Injest
from pipeline.injest import Population


class Driver: 
    ''' Sets up an UGLC processing pipline sourced from I3 files promoting processed hits to the specivied sink'''

    def __init__(self, consumer, mode=0):
        ''' 
        Set up an upgrade LC processing pipeline

            mode: 0=each frame is an independent unit of data
            mode: 1=the entire file set is a unit, pulse times will be offset to create a well ordered frame-by-frame hit stream
        '''
        self.consumer = consumer  # receives completed hits on a frame-by-frame basis
        self.mode = mode

    def process_all_files(self, files): 
        if self.mode == 0:
           self.__process_isolated(files)
        elif self.mode == 1:
            self.__process_joined(files)
     
    def __process_isolated(self, files): 
        ''' each frame is an independent unit of pulses with no defined correlation to other frames '''
        sw = Stopwatch()
        injest = Injest(files)
        cum_in=0
        cum_out=0
        for frame in injest.upgradePulseFrames(join=False):
            acc = Accumulator(self.consumer)
            acc.expectFrame(frame.frame_id, frame.rpsm)
           
            split_sw = Stopwatch()
            print(f'processing frame {frame.frame_id}...')
            omkeys = Population.extractPopulation(frame.rpsm)
            out_counter = Counter(acc)
            in_counter = Counter(Pipeline(out_counter, omkeys))
            for hit in frame.hits():
                in_counter.enque(hit)
            print(f'eos(frame) {frame.frame_id}...')
            in_counter.eos()
            cum_in += in_counter.cnt
            cum_out += out_counter.cnt
            print(f'Frame completed hits[ in:{in_counter.cnt} out:{out_counter.cnt} held:{in_counter.cnt - out_counter.cnt}] process time seconds {split_sw.elapsed()}')
       
        print(f'Processing completed hits[ in:{cum_in} out:{cum_out} held:{cum_in - cum_out}] process time seconds {sw.elapsed()}')

    

    def __process_joined(self, files): 
        ''' join all frames into a monotonic stream with each frame seperated by delta ticks '''
        acc = Accumulator(self.consumer)

        sw = Stopwatch()

        out_counter = Counter(self.sink)
        in_counter = None; # need the first frame(s) to learn the population
        

        injest = Injest(files)
        # peek the frames to learn the population
        peek = []
        for frame in injest.upgradePulseFrames(join=True):
            peek.append(frame.rpsm)
        omkeys = Population.extractPopulation(*peek)

        for frame in injest.upgradePulseFrames(join=True):
            acc.expectFrame(frame.frame_id, frame.rpsm)
            split_sw = Stopwatch()
            print(f'processing frame {frame.frame_id}...')
            
            if in_counter is None:
                in_counter = Counter(Pipeline(out_counter, omkeys))

            for hit in frame.hits():
                in_counter.enque(hit)
                
            print(f'Frame completed hits[ in:{in_counter.cnt} out:{out_counter.cnt} held:{in_counter.cnt - out_counter.cnt}] process time seconds {split_sw.elapsed()}')
        
        print(f'eos(all files)...')
        in_counter.eos()
        print(f'Processing completed hits[ in:{in_counter.cnt} out:{out_counter.cnt} held:{in_counter.cnt - out_counter.cnt}] process time seconds {sw.elapsed()}')




class FrameResult:
    ''' holds processed hits on a frame-by-frame boundary '''
    def __init__(self, frame_id, rpsm):
        # set up empty pulse series map
        self.frame_id = frame_id
        self.rpsm = rpsm
        t_start, t_end = Population.extractTimeInterval(rpsm)
        self.t_start = t_start
        self.t_end = t_end
        self.hits = []
        self.smlc_cnt = 0
        self.mmlc_cnt = 0


    def add(self, hit):
        self.hits.append(hit)
        if hit.smlc:
            self.smlc_cnt += 1
        if hit.mmlc:
            self.mmlc_cnt += 1
            
class Accumulator:
    '''Gathers processing output on a frame-by-frame basis'''

    def __init__(self, consumer):
        self.consumer = consumer  # completed frames will be sent here
        self.pending =  deque()   # holder for backlog of pendig frames

    def expectFrame(self, frame_id, rpsm):
        ''' called by the front end when a frame is pushed into the pipeline '''
        self.pending.append(FrameResult(frame_id, rpsm))

    def enque(self, hit):
        ''' collect processed hits int frames, releasing completed frames when ready '''
        t = hit.rawTime()
        while len(self.pending) > 0:
           if t < self.pending[0].t_start:
               raise RuntimeError(f'hit@ {t} earlier that earliest frame {self.pending[0].t_start}')
           if t > self.pending[0].t_end:
               self.consumer.consume(self.pending.popleft())
               continue
           else:
               self.pending[0].add(hit)
               return;

        raise RuntimeError(f'no frame found for hit@ {t}')


    def eos(self):
        # print(f'eos(acc) pending {len(self.pending)}')
        if len(self.pending) == 1:
            self.consumer.consume(self.pending.popleft())
        else:
            # should always end with a single in-flight fraems
            raise RuntimeError(f'eos does not match number pending frames {len(self.pending)}')

        print(f'Done processing')


