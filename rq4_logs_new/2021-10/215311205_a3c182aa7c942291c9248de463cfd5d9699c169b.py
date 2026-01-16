import suds
import ulmo
import pyhis

"""
FAILED: TRMM_3B42_7 - Type not found: 'queryInfo'
FAILED: EnviroDIY - HTTP Error 500: Internal Server Error
FAILED: NLDAS_FORA - Server raised fault: 'Unrecognized service. List of available method(s): GetSites GetSitesXml GetSiteInfo GetSiteInfoObject GetVariableInfo GetVariableInfoObject GetValues GetValuesObject'
FAILED: TarlandHydrology - <unknown>:138:7: mismatched tag
FAILED: WW2100 OHIS - <unknown>:138:7: mismatched tag
FAILED: NLDAS_NOAH - Server raised fault: 'Unrecognized service. List of available method(s): GetSites GetSitesXml GetSiteInfo GetSiteInfoObject GetVariableInfo GetVariableInfoObject GetValues GetValuesObject'
FAILED: OGIMET-ARCTIC - <unknown>:1:269: not well-formed (invalid token)
FAILED: GHCN - 'location'
FAILED: EPA_Lake_Harsha_Data - <urlopen error [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1056)>
FAILED: RushValley - HTTP Error 404: Not Found
"""



#wsdl = 'https://hydro1.gesdisc.eosdis.nasa.gov/daac-bin/his/1.0/NLDAS_FORA_002.cgi?WSDL'
#client = suds.client.Client(wsdl)

#for method in client.wsdl.services[0].ports[0].methods.values():
#    print('%s(%s)' % (method.name, ', '.join('%s: %s' % (part.type, part.name) for part in method.soap.input.body.parts)))


services = pyhis.Services()
providers = services.get_data_providers()
#import pdb; pdb.set_trace()
failed = {}
for idx, provider in providers.iterrows():
    name = provider['NetworkName']
    wsdl = provider['servURL']
    print(f'Querying {name}...', end='', flush=True)
    try:
        sites = ulmo.cuahsi.wof.get_sites(wsdl)
        print('success')
    except Exception as e:
        failed[name] = str(e)
        print('FAILED')

print('\nThe following providers failed during GetSites query')
for i, f in failed.items():
    print(f'{i}: {f}')

#def test_his_getsites():

