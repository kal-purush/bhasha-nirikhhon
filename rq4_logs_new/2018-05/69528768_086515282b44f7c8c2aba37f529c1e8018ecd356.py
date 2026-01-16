from ckanapi import RemoteCKAN

info = {
    'lifesafety': 'E00', #生活安全及品質
    'publicinformation': 'I00', #公共資訊
    'travel': '900', #休閒旅遊
    'investment': '800', #投資理財
    'traffic': 'A00', #交通及通訊
    'business': '600', #開創事業
    'job': '500', #求職及就業
    'medical': 'B00', #就醫
    'study': '300', #求學及進修
    'election': 'D00', #選舉及投票
    'birth': '200', #出生及收養
    'elderlycare': 'G00', #老年安養
    'fertility': '100', #生育保健
    'lifeetiquette': 'H00', #生命禮儀
    'housepurchase': 'C00', #購屋及遷徙
    'army': '400', #服兵役
    'marriage': '700', #婚姻
    'retirement':'F00' #退休
}

ua = 'ckanapiexample/1.0 (+http://140.110.141.160)'

ipgodTest = RemoteCKAN('http://140.110.141.160', apikey='3f10789c-9dcc-4db4-af6a-e9d7ac00ad7f')
packages = ipgodTest.action.package_list(id='data-explorer')

for package in packages:
    for key in ipgodTest.action.package_show(id = package)['extras']:
        if 'categoryCode' in key['key']:
            for infokey, infovalue in info.items():
                if key['value'] == infovalue:
                    print(package, key['value'], infokey)
                    ipgodTest.action.package_patch(
                        id = package, 
                        groups = [{'name':infokey}])