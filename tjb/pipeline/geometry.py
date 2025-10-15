#
# Stand-in for geometry database
#
#
from icecube import icetray, dataio, dataclasses, simclasses, phys_services, trigger_sim


class Geometry:

    class DeviceType(Enum):
        DEGG = 1
        MDOM = 2


    def __init__(self, table):
        self.table = table

    def lookup(self, omkey):
        return DEGG
