import json
import requests

def upload_image(path=None, title='', description='', auth='cb644a31670bc8e'):
    ''' This function will upload image to imgur

        Arguments:
        path:           Local path of the image
        title:          A name for the image (not required)
        description:    A descrtiption for the image (not required) 
        aut:            OAuth token for imgur, the default value will work 

        Return:
        Json with status, success and link of the image. All the info comes from
        imgur API   
    '''

    ret_val = None

    if path is None:
        ret_val = json.dumps({'status': '400',
                                'success': False,
                                'link': ''})
    else:
        with open(path, 'rb') as fp:
            image = fp.read()
        payload = {'image': image,
                    'title': title,
                    'description': description}
        headers = {'Authorization': 'Client-ID {}'.format(auth)}
        response = requests.post(url='https://api.imgur.com/3/image',
                                  headers=headers,
                                  data=payload).json()
        ret_val = json.dumps({'status': response['status'],
                                'success': response['success'],
                                'link': response['data']['link']})

    return ret_val