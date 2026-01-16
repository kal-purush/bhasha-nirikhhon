
import builtins, os, sys, urllib.request

def import_gdoc(docid):
    doc_url = "https://docs.google.com/document/d/"+docid+"/export?format=txt"
    doc = urllib.request.urlopen(doc_url, timeout =60).read().decode('utf-8')
    doc = doc.replace("“",'"').replace("”",'"')
    doc = doc.replace("‘","'").replace("’","'")
    doc = doc.replace("\r","")
    doc = doc.encode('utf-8')
    return doc

def main(path):

    running_print = [""] #put this inside main so that running_print gets reset everything you press refresh
    def orig_print(x): #without html break characters
        if type(x) == bytes:
            x = x.decode("utf-8") 
        if running_locally:
            builtins.print(x)
        else:
            running_print[0] = running_print[0] + str(x)
    def print(x): #with html break characters
        if type(x) == bytes:
            x = x.decode("utf-8") 
        if running_locally:
            builtins.print(x)
        else:
            running_print[0] = running_print[0] + str(x) + str("<br>")
            
    if path == "": #homepage
        return "Alive"
    else:
        try:
            exec(import_gdoc(path))
        except:
            print(sys.exc_info())
    return running_print[0]

if __name__ == '__main__':
    running_locally = True
    starter_url = "SET URL HERE"
    main(starter_url)
else:
    running_locally = False
    #try:
    from flask import Flask
    app = Flask(__name__)
    @app.route('/', defaults={'path': ''})
    @app.route('/<path:path>')
    def flash_main(path):
        return main(path)
    #except:
    #    app = object()
    #    pass
