import urllib.parse
import urllib.request
import argparse
import base64
import json


def SendTextToSpeechServer(url, Wavfile):
    with open(Wavfile, 'rb') as wav:
        wav_content = base64.b64encode(wav.read())

    webdata = {'Wavfile': "Lol.ogg", 'EncodedSpeech': wav_content}
    print(webdata)
    params = urllib.parse.urlencode(webdata).encode("utf-8")
    response = urllib.request.urlopen(url, params)
    json_response = response.read()

    print("\nRequest: %s" % Wavfile)
    print("Response: %s" % json_response)

    j = json.loads(json_response)
    decodetext = j['decodeText'].rstrip('\n')
    print("decodeText:", decodetext, ", time elapsed:", j['timeElapse'])

##############################################################################################################

# parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
# parser.add_argument('Wavfile', help='Recorded Wav File location')
# args = parser.parse_args()
#
# Wavfile = args.Wavfile
# print "Wavfile: %s" %(Wavfile)


url = 'http://192.168.51.206:8000/speech/english/imda'    # test IMDA ASR
SendTextToSpeechServer(url, "test.ogg")