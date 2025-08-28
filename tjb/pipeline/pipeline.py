# processing pipeline to route hits into upgrade LC code modules hit-by-hit in a pdaq compatible way 

import time

from collections import deque

from pipeline.injest import Population
from pipeline.injest import ModuleKey
from pipeline.injest import MyHit
from uglc.smlc import SMLC


class Pipeline:
    '''Builds a processing pipeline to iterate RecoPulsSeriesMap(s) in pdaq-order and and mark UGLC status'''

    def __init__(self, sink, all_omkeys):
        # builds up a Sorter-->Demuxer-->SMLC->Sorter pipeline that will drive
        # hits from rpsm to smlc instances in time order then to supplied sink in
        # time order
        #
        # completed marked hits will be passed to "sink" in time order
        #
        # rpsm is not stored, used to dynamically learn the omkey population
        # needed to plumb the pipeline
        #

        # om_keys: overall  channel population
        # by_module: omkeys grouped by module
        self.om_keys = all_omkeys
        self.by_module = Population.byModule(self.om_keys)

        # assemble the processing pipeline
        self.sink = sink                                                                            # sink:        The terminal node: receives the processed hits in time order
        self.sorter_out = Sorter(self.by_module.keys(), sink)                                 # sorter_out:  Receives the processed hits from each SMLC, sorts on time and passes to terminal node
       

        # build an input map that feeds each omkey stream to a per-module SORT/SMLC pipeline        # a per-module SORT/SMLC pipeline with per-omkey inputs
        self.by_omkey_input = {}
        for k in self.by_module.keys():
            modulesort = Sorter(self.by_module[k], SMLC(k, SMLC.SMLCConfig(100), self.sorter_out.inputFor(k)))
            for omk in self.by_module[k]:
                self.by_omkey_input[omk] = modulesort.inputFor(omk);


        self.demux = Demuxer(self.by_omkey_input)                                                    # demux:       Demuxes a stream by omkey, pushing hits to the correct OMKEY/SORT/SMLC pipelines
        
        self.input_node = self.demux                                                                 # input_node alias the demuxer as "input node" 



        # Debugging examples
        # self.input_node = OMFilter( EnforceOrdering(self.input_node, "stage-1"), 89, 66, 1)            # filter input to a single module
        # self.input_node =PMTFilter( self.input_node, 1)                                                # filter input to a single channel


    def enque(self, hit):
        ''' input a hit into the pipeline '''
        self.input_node.enque(hit);

    def eos(self):
        ''' signals the end of inputs, flushes pipeline '''
        self.input_node.eos()



def ensureSink(obj):
    ''' checks if object implements the pipeline interface'''
    if hasattr(obj, 'enque') and hasattr(obj, 'eos'):
        pass
    else:
        raise RuntimeError(f'Not a sink: {obj}');


class Demuxer:
    '''' demuxes hits from a unified stream to a stream-per omkey'''
    def __init__(self, sinks):
            for s in sinks.values():
                ensureSink(s)
            self.sinks = sinks


    def enque(self, myhit):
        if myhit.omkey not in self.sinks:
            raise RuntimeError(f"OMKey {myhit.omkey} not in sink dict")

        # suport using a sentinel hit to trigger per-steeam EOS 
        if not myhit.isEOS():
            self.sinks[myhit.omkey].enque(myhit)
        else:
            self.sinks[myhit.omkey].eos()
            

    def eos(self):
        ''' Call eos on all sinks'''
        for sink in self.sinks.values():
           sink.eos()


class Counter:
    '''Sanity check'''

    def __init__(self, sink):
        # set up count
        self.cnt = 0
        self.sink = sink

    def enque(self, hit):
        # increases count 
        self.cnt += 1
        self.sink.enque(hit)

    def eos(self):
        ''' Call at end of data stream to flush all remaining hits to the sink'''
        self.sink.eos()

class Stopwatch:
    '''stopwatch'''

    def __init__(self):
        # set up count
        self.__start_t = time.monotonic()

    def start(self):
        self.__start_t  = time.monotonic()

    def elapsed(self):
       now = time.monotonic()
       return now - self.__start_t 

class EnforceOrdering:
    '''Sanity check'''

    def __init__(self, sink, name="order-check"):
        # set up count
        self.last_hit = None
        self.sink = sink
        self.name = name

    def enque(self, hit):
        if self.last_hit is not None and self.last_hit.resolveTime() > hit.resolveTime():
            raise RuntimeError(f'Out of order hit at {self.name}. last: {self.last_hit.omkey} {self.last_hit.resolveTime()}, current:  {hit.omkey} {hit.resolveTime()}, Delta t={self.last_hit.resolveTime() - hit.resolveTime()}')
        self.last_hit = hit
        self.sink.enque(hit)

    def eos(self):
        ''' Call at end of data stream to flush all remaining hits to the sink'''
        self.sink.eos()

class PMTFilter:
    '''filters pmts'''

    def __init__(self, sink, pmt):
        # set up count
        self.sink = sink
        self.pmt = pmt
        self.passed = 0
        self.dropped = 0

    def enque(self, myhit):
        if myhit.omkey.pmt == self.pmt:  
            # print(f'pass {myhit.resolveTime()} [{myhit.omkey.string}-{myhit.omkey.om}-{myhit.omkey.pmt}]')
            self.sink.enque(myhit)
            self.passed += 1
        else:
            self.dropped += 1

    def eos(self):
        ''' Call at end of data stream to flush all remaining hits to the sink'''
        print(f'PMTFilter: passed: {self.passed} Dropped Hits: {self.dropped}')
        self.sink.eos()


class OMFilter:
    '''filters pmts'''

    def __init__(self, sink, string, om, pmt):
        # set up count
        self.sink = sink
        self.string = string
        self.om = om
        self.pmt = pmt

    def enque(self, myhit):
        if myhit.omkey.string == self.string and myhit.omkey.om == self.om and myhit.omkey.pmt == self.pmt:  
            # print(f'pass {myhit.resolveTime()} [{myhit.omkey.string}-{myhit.omkey.om}-{myhit.omkey.pmt}]')
            self.sink.enque(myhit)

    def eos(self):
        ''' Call at end of data stream to flush all remaining hits to the sink'''
        self.sink.eos()
     
        
class Stop:
    '''Development utility to truncate the pipeline at a particular stage'''

    def __init__(self):
        pass

    def enque(self, hit):
        pass

    def eos(self):
        pass

class LoggingStage:
    '''Development utility to log hits at particular stage'''

    def __init__(self, name, sink):
        self.name = name
        self.sink = sink

    def enque(self, hit):
        print(f'hit from {hit.omkey} time: {hit.resolveTime()} @ {self.name}')
        self.sink.enque(hit)

    def eos(self):
        print(f'EOS @ {self.name}')
        self.sink.eos()


class Joiner:
    '''Join Demuxed streams'''

    class Input:
        def __init__(self, joiner, key):
            self.joiner=joiner
            self.key=key
            self.iseos=False

        def enque(self, hit):
            self.joiner.propagate(hit)

        def eos(self):
            self.iseos=True;
            self.joiner.eos(self.key);

        
    def __init__(self, keys, sink):
        self.sink = sink
        self.input_nodes = {}
        for k in keys:
           self.input_nodes[k] = Joiner.Input(self, k);

    def inputFor(self, key):
        if key in self.input_nodes.keys():
            return self.input_nodes[key]
        else:
            raise RuntimeError(f'Joiner not plumbed for {key}')

    def propagate(self, hit):
        self.sink.enque(hit)

      
    def eos(self, key):
        ''' hold eos status until all inputs streams has eos()'d'''
        print(f'Joiner EOS: from: {key}')
        for k, input_node in self.input_nodes.items():
            if(not input_node.iseos):
                return
    
        self.sink.eos()


# O(n^2), super slow
class Sorter:
    '''time sort multiple time-ordered streams into one time ordered stream'''

    class InputNode:
        def __init__(self, sorter, key):
            self.key=key
            self.sorter=sorter
            self.hits = deque()
            self.iseos = False

        def enque(self, hit):
            self.hits.append(hit);
            self.sorter.releaseAvailable()

 
        def eos(self):
            # print(f'eos({self.key})')
            if self.iseos:
                raise RuntimeError(f'duplicate eos({self.key}')

            self.iseos=True;
            self.sorter.eos(self.key)

        def earliest(self):
            if(len(self.hits) > 0):
                return self.hits[0].resolveTime()
            else:
                return None

        def pop(self):
            return self.hits.popleft();


    def __init__(self, keys, sink):
        self.sink = sink
        self.input_nodes = {}
        for k in keys:
           self.input_nodes[k] = Sorter.InputNode(self, k);
        self.inn = 0;
        self.out = 0;

    def inputFor(self, key):
        if key in self.input_nodes.keys():
            return self.input_nodes[key]
        else:
            raise RuntimeError(f'Sorter not plumbed for {key}')

    def eos(self, key):
        ''' accept eos from an input node  '''
        # flush and propagate if all streams are eos
        self.releaseAvailable()
        for k, input_node in self.input_nodes.items():
            if(not input_node.iseos):
                return
        
        self.sink.eos()
        
     
    def releaseAvailable(self):
        ''' Release hits available hits in time-sorted order'''
        ''' return the earliest hit '''
        # bucket queue approach
        while(True):
            t_min = None
            ch = None
            for key, node in self.input_nodes.items():
                t = node.earliest()
                if t is not None:
                    if t_min is None or t < t_min:
                       t_min = t
                       ch = key
                elif not node.iseos:
                    return;             # an in-flight empty node prevents release
             
            if(ch is not None):
                self.sink.enque(self.input_nodes[ch].pop())
            else:
                return                 # All nodes empty and EOS



