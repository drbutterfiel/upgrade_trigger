#
# methods for injesting RecoPulsSeriesMap data from i3 files into pipeline domain objects.
# There may be better performing or otherwise preferred or built-in mechanisms in icetray
#
#
from icecube import icetray, dataio, dataclasses, simclasses, phys_services, trigger_sim


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


class Grouping:
    ''' grouping for hits, traces back to frame/file to manage time offsetting'''

    def __init__(self, id, t_offest):
        self.id = id                  # probably "file:frame"
        self.t_offest = t_offest      # the time offset to apply in resolveTime



class MyHit:
    ''' Hit with OMKey joined to it

        Models a pdaq hit record
        Hides direct access to recopulse

    '''

    def __init__(self, group, omkey, recopulse):
        self.group = group
        self.omkey = omkey
        self.__recopulse = recopulse
        self.smlc = False
        self.mmlc = False


    def resolveTime(self):
        ''' timestamp accessor

            timestamp accessed indirectly to support chaining frames into a long-running -strea 
        '''
        return self.__recopulse.time + self.group.t_offest      # coerce long monotonic data streams by offesetting pulse timestamps

    def rawTime(self):
        ''' raw recopulse timestamp accessor, for debugging group offset '''
        return self.__recopulse.time


    def isEOS(self):
        ''' prospective, could support channel-drop by signalling a per-channel eos to sorter/joiner'''
        return False

    def markSMLC(self):
        self.smlc = True

    def markMMLC(self):
        self.mmlc = True

class Population:
    ''' Methods to characterize the data population of a ReconPulseSeriesMap '''

    def extractPopulation(*rpsms):
        ''' learn the population of omkeys in a group of pulse series

            multiple rpsm are needed for pipelines spanning frames b/c the first frame is not representational

        '''
        allOMKeys = set()

        for rpsm in rpsms:
            om_keys = rpsm.keys()
            for omkey in om_keys:
                allOMKeys.add(omkey)

        return list(allOMKeys)

    def byModule(omkeys):
        ''' index om keys by module '''
        by_module = {}

        for omkey in omkeys:
            module_key = ModuleKey.extractOMKey(omkey)
            if module_key not in by_module.keys():
                by_module[module_key] = set()
            by_module[module_key].add(omkey)

        return by_module

    def extractTimeInterval(rpsm):
        ''' learn t_min, t_max '''
        t_min = None
        t_max = None
        for k, l in rpsm.items():
            if len(l) > 0:
                if t_min is None or l[0].time < t_min:
                    t_min = l[0].time
                if  t_max is  None or l[-1].time > t_max:
                    t_max = l[-1].time

 
        if t_min is None or t_max is None:
            raise RuntimeError("Deficient series, No pulses")

        return (t_min, t_max)



class Frame:
    ''' Provide RecoPulseSeriesMap iterations'''
    def __init__(self, group, rpsm):
        self.frame_id = group.id
        self.group = group
        self.rpsm = rpsm

    def hits(self):
        ''' iterate the frame to produce a stream of MyHits '''

        # not clear if iteration order matters given that a frame time interval is short
        # but two iteration options are implemented

        # yield from self.__hits_breathFirst()
        yield from self.__hits_depthFirst()

    def __hits_breathFirst(self):
        ''' iterate evenly, pulling hits evenly by channel '''
        keys = list(self.rpsm.keys())
        pulseseries = list(self.rpsm.values())

        i = 0;
        while(len(keys)>0):
            removals = []
            for j, k in enumerate(keys):
                try:
                  pulse = pulseseries[j][i]
                  yield MyHit(self.group, k, pulse)
                except:
                    # list exhausted for this channel
                    # yield MyHit(self.group, k, None) # eos
                    removals.append(j)

            for r in sorted(removals, reverse=True):
                keys.pop(r)
                pulseseries.pop(r)
            removals = []
            i+=1;

    def __hits_depthFirst(self):
        ''' iterate channel-by-channel'''
        for omkey, pulses in self.rpsm.items():
            for pulse in pulses:
                yield MyHit(self.group, omkey, pulse)
               

class Injest:
    ''' Injest I3Files and produce hit streams '''

    def __init__(self, files):
        self.files = files

    def upgradePulseFrames(self, join=False, delta=100):
        ''' iterate the "upgrade" frames in the files'''
        if not join:
            yield from self.__unjoined()
        else:
            yield from self.__joined(delta)


    def __unjoined(self):
        ''' iterate the "upgrade" frames in the files without joining into monotonic stream'''
        for fname in self.files:
            f = dataio.I3File(fname)
            cnt = 0;
            for frame in f:
                if frame.Stop == icetray.I3Frame.DAQ:
                    if 'I3RecoPulseSeriesMapUpgrade' in frame:
                        rpsm = frame['I3RecoPulseSeriesMapUpgrade']
                        group = Grouping(f'{fname}:{cnt}', 0)  # each frame is independent
                        yield(Frame(group, rpsm))
                        cnt += 1

    def __joined(self, delta):
        ''' iterate the "upgrade" frames in the files, joining into monotonic streams via the Group'''
       
        last_pit = 0;
        for fname in self.files:
            f = dataio.I3File(fname)
            cnt = 0;
            for frame in f:
                if frame.Stop == icetray.I3Frame.DAQ:
                    if 'I3RecoPulseSeriesMapUpgrade' in frame:
                        rpsm = frame['I3RecoPulseSeriesMapUpgrade']
                        t_min, t_max = Population.extractTimeInterval(rpsm);
                        offset = (last_pit - t_min) + delta
                        group = Grouping(f'{fname}:{cnt}', offset)  # track the inter-group time offset
                        #print(f'DEBUG: frame {cnt} interval: [{t_min}-{t_max}] last-pit: {last_pit} time_offset: {offset} ---> interval: [{t_min + offset}-{t_max + offset}]')
                        yield(Frame(group, rpsm))
                        last_pit = last_pit + (t_max + offset)
                        cnt += 1


