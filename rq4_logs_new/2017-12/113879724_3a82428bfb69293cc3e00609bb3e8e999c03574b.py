class output:
    
    def __init__(self,call,sample):
        self.call = call
        self.sample = sample
    def loadregions(self):
        self.regions = {}
        with open('locationData.tsv') as locationData:
            for line in locationData:
                if '##REGION' in line:
                    line = line.split('=')
                    line = line[1].split(':')
                    line = line[1].split(';')
                    #print(line[0])
                    self.regions[line[1]] = line[0].split('-')
        #for key, value in regions.items():
            #print(key,value)
        return self.regions

    def tofile(self,regions):
        print(self.sample)
        print("Hap1 Shared")
        for hap, variant in self.call.hap1call.items():
            print(hap)
            for loc in variant:
                for key,value in regions.items():
                    location = loc.split('~')
                    if int(location[1]) >= int(value[0]) and int(location[1]) <= int(value[1]):
                        print(key)
                        print(loc)





        