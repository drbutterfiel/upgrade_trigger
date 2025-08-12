import numpy as np 
import math 
import os.path 
import glob
import h5py
import pandas as pd
import pickle
import matplotlib.colors as colors
import argparse
import os
import json
import gzip
import dask.dataframe as dd
from icecube import icetray, dataio, dataclasses, simclasses, phys_services, trigger_sim
import sys

class Driver: # PRIORITY #
    '''Could do something like unpack file, sort / demux, etc., builds processing pipeline'''
    # def __init__(all_omkeys):
    #     # initialize new sw instance
    #     self.sw = SlidingWindow(sink)
    #     self.smlc = SMLC() # eventually will have arg
    #     self.acc = Accumulator() # this is where marked hits end up
    #     self.demuxer = Demuxer(acc, all_omkeys) # this takes rpsm, "sorts" it into time ordered stream by om
    #     # also known as setting up pipeline

    def __init__(self):
        self.acc = Accumulator()
        self.demuxer = None  # Will be initialized after reading the file
        self.counter = None
        

    def process_file(self, filename):
        print(f'Processing file: {filename}')
        rpsm, all_omkeys = self.ingest_file(filename)
        if self.demuxer is None:
            self.counter = Counter(self.acc)
            self.demuxer = Demuxer(self.counter, all_omkeys)
        self.demuxer.demux(rpsm)
        # need to inform accumulator of file boundary
        self.acc.file_sync_point(filename, './test_outfile.i3.zst', self.demuxer.pit)
        

    # take file, demux, attach to SMLC, remux (?)
    # initializes data flow & builds processing pipeline
    # helps debug sliding window
    # each step of the pipeline should have its own function

    def process_all_files(self, files):        
        for file in files:
            self.process_file(file)
            self.print_status()
        self.demuxer.eos()
        # hack: work around multiple smlc eos, we tell accumulator eos instead
        self.acc.eos()
        self.print_status()
        

    def ingest_file(self, filename):
        f = dataio.I3File(filename)
        for frame in f:
            if frame.Stop == icetray.I3Frame.DAQ:
                if 'I3RecoPulseSeriesMapUpgrade' in frame:
                    rpsm = frame['I3RecoPulseSeriesMapUpgrade']
                    all_omkeys = list(rpsm.keys())
                    return rpsm, all_omkeys
        raise RuntimeError("No pulse map found")

    def print_status(self):
        '''Check accounting'''
        print(f"Accumulator Count = {self.acc.cnt}")
        print(f"Counter Count = {self.counter.cnt}")
        print(f"Held hits = {self.counter.cnt - self.acc.cnt}")

    def write_file(self):
        


        
        infile = dataio.I3File(infile)
        outfile = dataio.I3File(outfile, dataio.I3File.Mode.Writing)

        ## insert new frame objects to outfile here ##
        for frame in outfile:
            frame['New RPSM'] = new_rpsm

        outfile.close()
        infile.close()


class ModuleKey:
    '''Identifies individual modules by (string, om)'''

    def __init__(self, string, om):
        self.string = string
        self.om = om

    def __hash__(self):
        return hash((self.string, self.om))

    def __eq__(self, other):
        return (self.string, self.om) == (other.string, other.om)

    def __str__(self):
        return f'{(self.string, self.om)}'

    def extractOMKey(omkey):
        return ModuleKey(omkey.string, omkey.om)

class MyHit:
    '''Hit with OMKey joined to it'''

    def __init__(self, omkey, recopulse):
        self.omkey = omkey
        self.recopulse = recopulse        

class Demuxer:
    '''Sorts / demuxes I3RecoPulseSeriesMap into time ordered stream of hits'''

    def __init__(self, sink, all_omkeys):
        # builds up demuxer that will demux hits in rpsm to smlc instances
        # completed marked hits will be passed to "sink"
        # rpsm is not stored, used only to build dictionary
        # self.om_dict # iterate all omkeys making an smlc for each (string, om)
        self.om_dict = {}
        for omkey in all_omkeys:
            module_key = ModuleKey.extractOMKey(omkey)
            if module_key not in self.om_dict:
                # print(f'Adding {module_key} to OM Dict')
                # self.om_dict[module_key] = SMLC(sink)
                self.om_dict[module_key] = PMTFilter(SMLC(sink), 4)
                
                # need to make an SMLC sink
        self.pit = -1
        
    def demux(self, rpsm):
        # demuxes from (string, om, channel) to (string, om)
        # orders all hits on single (string, om) combination

        # TODO:
        # rpsm --> need to iterate in time order by module
        # currently stored as time ordered by channel

        for omkey, pulses in rpsm.items():
            module_key = ModuleKey.extractOMKey(omkey)
            if module_key not in self.om_dict:
                raise RuntimeError(f"Module {module_key} not in OM Dict")
            for pulse in pulses:
                # should we make a copy of the class?
                # hit = dataclasses.I3RecoPulse(time=pulse.time, string=omkey.string, om=omkey.om)
                self.om_dict[module_key].enque(MyHit(omkey, pulse))
                if self.pit < self.om_dict[module_key].pit:
                    self.pit = self.om_dict[module_key].pit 
                # TODO: cannot track point in time on unsorted stream, but we can track latest p.i.t
                

    def eos(self):
        ''' Call eos on all SMLCs'''
        for smlc in self.om_dict.values():
            smlc.eos()


class SlidingWindow:
    ''' Sliding window test class'''

    def __init__(self, sink): 
        self.window_length = 100 # ns (not configurable)
        self.hits = []
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
        # hit will be I3RecoPulse
        # enforce monotonic time increase
        if self.curr_time != None and self.curr_time > myhit.recopulse.time:
            raise RuntimeError(f"Out of order hits. OMKey 1 = {myhit.omkey}, OMKey 2 = {self.prev_hit.omkey}. Current time={self.curr_time}, Previous time={myhit.recopulse.time}, Delta t={self.curr_time - myhit.recopulse.time}")
            
        self.curr_time = myhit.recopulse.time

        # check to see if hits are within time window
        while len(self.hits) != 0 and self.curr_time - self.hits[0].recopulse.time > self.window_length:
            old_hit = self.hits.pop(0)
            self.sink.enque(old_hit)

        self.hits.append(myhit)
        self.prev_hit = myhit

    def eos(self):
        ''' Call at end of data stream to flush all remaining hits to the sink'''
        while self.hits:
            self.sink.enque(self.hits.pop(0))
        # self.sink.eos()

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


class SMLC:
    ''' Only marks the hits, does not release hits or move the window'''
    
    def __init__(self, sink):
        self.sw = SlidingWindow(sink)
        # SMLC sink is where marked / flagged data goes = marked_hit_dest

    def enque(self, hit):
        # receives / consumes a time ordered stream of hits from a single module
        self.sw.enque(hit)
        self.examine(self.sw.hits) # run SMLC algorithm to flag hits

    def examine(self, window_hits): # PRIORITY #
        # TODO: Implement actual SMLC logic here.
        pass

    def eos(self):
        ''' Call at end of data stream to flush all remaining hits to the sink'''
        self.sw.eos()

class Accumulator:
    '''Temporary sink for dev purposes'''

    def __init__(self):
        # set up empty pulse series map
        self.rpsm = dataclasses.I3RecoPulseSeriesMap()
        self.cnt = 0
        self.pit = -1
        self.infile = None
        self.outfile = None
        self.last_pit = None

    def enque(self, hit):
        # "done" hits will appear here in time order ***time ordered by module (not global time ordered)
        # "done" = passed through SMLC algorithm
        # TODO: get time ordered in global time ordered
        # write pulse series map and put back into file or new frame object

        # throw hits into map by OMKey
        #self.psm = 
        self.cnt += 1
        self.pit = hit.recopulse.time
        self.check()

    def file_sync_point(self, infile, outfile, last_pit):
        if last_pit < self.pit:
            raise RuntimeError(f"Current time {self.pit} later than requested time {last_pit}, delta t = {self.pit - last_pit}")
            
        self.infile = infile
        self.outfile = outfile
        self.last_pit = last_pit
        

    def check(self):
        '''Check if current file being processed has made it entirely through accumulator'''
        if self.last_pit == None:
            return
        if self.pit > self.last_pit:
            ## write outfile ##
            # copy rpsm from infile & add new stuff
            # integrate into outfile
            # close outfile
            print(f'Writing outfile to {self.outfile}, hit count: {self.cnt}')
            self.cnt = 0
            
            

    def eos(self):
        ''' Call at end of data stream to flush all remaining hits to the sink'''
        # while self.hit:
        #     self.sink.enque(self.hits.pop(0))
        # TODO: new eos for Accumulator
        print(f'Done processing. Hits in Accumulator = {self.cnt}')
        # Need to account for last datum in last file
        # hack: set current p.i.t. to be 1 greater than it is to fake getting a hit
        self.pit += 1
        print(f"File sync point in time: {self.last_pit}, Current point in time: {self.pit}")
        self.check()


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

class PMTFilter:
    '''filters pmts'''

    def __init__(self, sink, pmt):
        # set up count
        self.sink = sink
        self.pmt = pmt
        self.dropped = 0
        self.pit = -1

    def enque(self, myhit):
        if myhit.omkey.pmt == self.pmt:  
            self.sink.enque(myhit)
            self.pit = myhit.recopulse.time
        else:
            self.dropped += 1

    def eos(self):
        ''' Call at end of data stream to flush all remaining hits to the sink'''
        # print(f'Dropped Hits: {self.dropped}')
        self.sink.eos()
        
        