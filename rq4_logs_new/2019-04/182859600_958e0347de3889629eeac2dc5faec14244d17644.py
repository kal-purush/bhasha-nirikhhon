from wox import Wox,WoxAPI
import pyperclip
import json

def json_wox(title, subtitle, icon, action=None, action_params=None, action_keep=None):
    json = {
        'Title': title,
        'SubTitle': subtitle,
        'IcoPath': icon
    }
    if action and action_params and action_keep:
        json.update({
            'JsonRPCAction': {
                'method': action,
                'parameters': action_params,
                'dontHideAfterAction': action_keep
            }
        })
    return json

def copy_to_clipboard(text):
    pyperclip.copy(text.strip())

def check_properties(query):
    results = []
    kw = query.split(" ")

    app = kw[0]
    word = ""
    if len(kw) >1 :
        word = kw[1]
    with open("config.json", "r", encoding="utf-8") as f1:
        a = json.load(f1, encoding="utf-8")
        for key in a.keys():
            if key.lower().find(app.lower()) != -1 or app == '':
            	results.append(json_wox(key,
            	                a[key]['desc'],
            	                'Images/app.png',
            	                'copy_clip',
            	                [str(a[key]['url'])],
            	                True))
    return results

class Properties(Wox):

    def query(self, query):
        return check_properties(query)

    def copy_clip(self, query):
        #copy to clipboard after pressing enter
        copy_to_clipboard(query)
        WoxAPI.hide_app()

if __name__ == "__main__":
    Properties()