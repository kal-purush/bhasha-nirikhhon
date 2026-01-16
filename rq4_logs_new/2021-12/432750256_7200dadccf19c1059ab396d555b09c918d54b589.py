class NewDict(dict):
    def __repr__(self):
        print('repr metondundan mesaj var.')
        return super().__repr__()
    
    def __missing__(self,key):
        print('olmayan key bilgisi istiyorsunuz.')
        
        
    def __getitem__(self,key):
        print('bir elemani cagiriyorsunuz.')
        return super().__getitem__(key)
    
    def __setitem__(self,key,value):
        print('Listeye eleman ekliyorsunuz.')
        super().__setitem__(key,value)
        
    def __contains__(self,item):
        print('Listeye eleman arayamasiniz.')
        return super().__contains__(item)
        
date = NewDict({'first':'Orkhan','last':'Mustafayev'})

print(date)
print(date['first'])
date['age']


print('a' in date)