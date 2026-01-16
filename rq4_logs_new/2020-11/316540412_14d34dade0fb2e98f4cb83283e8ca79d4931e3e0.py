import os
import time
import flask
import subprocess

# Create an instance of Flask
app = flask.Flask(__name__)

@app.route("/images", methods=['POST'])
def images():
    for file in flask.request.files.values():
        filename = os.path.join(".", str(int(time.time())) + "_" + file.filename)
        file.save(filename)    
        result = tf_classify(filename)
        os.remove(filename)
        return flask.jsonify(result)
    return flask.jsonify([])

def tf_classify(name):
    print("detecting %s..." % name)
    p = subprocess.Popen(["python", "./models/tutorials/image/imagenet/classify_image.py", "--model_dir=.", "--image_file=" + name], 
        stdout=subprocess.PIPE)
    out, _ = p.communicate()
    return out.split("\n")[1:-1]

if __name__ == "__main__":
    app.run(host="0.0.0.0")