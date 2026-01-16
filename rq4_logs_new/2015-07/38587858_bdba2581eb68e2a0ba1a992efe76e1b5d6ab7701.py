from operator import itemgetter

class Solution(object):
    def __init__(self, elapsed_time, schedule):
        self.elapsed_time = elapsed_time
        self.schedule = schedule

        # TODO: unique images
        # TODO: total compute time

    def save(self, path):
        '''Saves a DICP solution to a json file'''
        # json formatting doen't make it very human readable, so we do our own.
        with open(path, 'w') as fp:
            fp.write('{\n')
            fp.write('    "elapsed_time": %f,\n' % self.elapsed_time.total_seconds())
            fp.write('    "schedule": {\n')
            for j, (i, v) in enumerate(sorted(self.schedule.items(), key=itemgetter(0))):
                if j == len(self.schedule)-1:
                    end = '\n'
                else:
                    end = ',\n'
                fp.write(('        "%s": %r%s' % (i, v, end)).replace("'",'"').replace('u',''))
            fp.write('    }\n')
            fp.write('}\n')