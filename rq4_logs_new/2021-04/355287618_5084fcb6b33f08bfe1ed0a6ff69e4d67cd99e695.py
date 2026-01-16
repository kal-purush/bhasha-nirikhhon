class FastService:
    '''
    This service does stuff fast
    '''
    def fast(self, name):
        print(f'Going really fast for {name}')


'''
This will start the service
'''
if __name__ == "__main__":
    print("Starting up your fast service...")

    fs = FastService()
    fs.fast('you')