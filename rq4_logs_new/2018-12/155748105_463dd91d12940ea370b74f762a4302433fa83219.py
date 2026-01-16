from watson_developer_cloud import SpeechToTextV1
import json


def recognize_speech_ibm_plus(yourkey, url_address, audio_input):
    # yourkey = '4b9ZXrpMUL6jo5Q9lw1htmo_Y4xdiku9tydRKVY8Q9Pv'
    # url_adress = 'https://stream.watsonplatform.net/speech-to-text/api'
    speech_to_text = SpeechToTextV1(
        iam_apikey=yourkey,
        url=url_address
    )

    # files = 'C:/Users/taotao/Desktop/research/test/test.wav'
    files = audio_input
    with open(files, 'rb') as audio_file:
        speech_recognition_results = speech_to_text.recognize(
            audio=audio_file,
            model="en-US_BroadbandModel",
            content_type='audio/wav',
            timestamps=True,
        ).get_result()
    print(json.dumps(speech_recognition_results, indent=2))


recognize_speech_ibm_plus('4b9ZXrpMUL6jo5Q9lw1htmo_Y4xdiku9tydRKVY8Q9Pv',
                          'https://stream.watsonplatform.net/speech-to-text/api', "C:/Users/taotao/Desktop/research/test/step1.wav")
