#
# Test the pipeline mechanics
#
#
#

from pipeline.driver import Driver

# completed frames end up here
class FrameConsumer:

    def consume(self, frame):
        print(f'---------------------------------------------------------------')
        print(f'COMPLETED FRAME: {frame.frame_id}')
        print(f'TIME_INTERVAL: [{frame.t_start} - {frame.t_end}]')
        print(f'TIME_LEN: {frame.t_end - frame.t_start}')
        print(f'HITS: {len(frame.hits)}')
        print(f'SMLC: {frame.smlc_cnt}')
        print(f'MMLC: {frame.mmlc_cnt}')
        print(f'---------------------------------------------------------------')



def run():
    
    # Initiate the driver
    driver = Driver(FrameConsumer(), 0) # independent frames
    # driver = Driver(FrameConsumer(), 1) # frames joined into a cohesive monotonic time sequenece

    # Path to your test file
    test_files = ['/data/sim/IceCube/2023/generated/RandomNoise/23221/0000000-0000999/RandomNoise_IceCubeUpgrade_v58.23221.0.i3.zst']

    # Process the file
    driver.process_all_files(test_files)
