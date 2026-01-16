from flask import Flask, render_template, request, redirect, url_for, jsonify, Response, send_from_directory
import os
from dbconnect import Connection
from traintest import Calculate
from pydub import AudioSegment
from test_separate import calculate_voice
import subprocess
import wave
app = Flask(__name__)

# displaying icon in Title


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

# error handlers


@app.errorhandler(404)
def page_not_found(e):
    return render_template("404.html")


@app.errorhandler(405)
def method_not_found(e):
    return render_template("405.html")


@app.errorhandler(500)
def internal_server_error(e):
    return render_template("500.html")

# home page


@app.route('/')
@app.route('/index.html')
def index():
    return render_template('index.html')

# upload page


@app.route('/upload.html')
def uploadpage():
    try:
        con = Connection()
        return render_template('upload.html', success="Successfully Connected to Database", error=False)
    except Exception as e:
        return render_template('upload.html', success=False, error=e)

# traintest page


@app.route('/traintest.html')
def traintestpage():
    return render_template('traintest.html')

# files in database page


@app.route('/fidb.html')
def files_in_database():
    try:
        con = Connection()
        cur = con.cursor(buffered=True)
        cur.execute("select * from audio_info order by audio_name")
        if (cur.rowcount > 0):
            return render_template('fidb.html', fidb=True, result=cur, error=False)
        else:
            return render_template('fidb.html', fidb=False, error=True, message="No files in database!")
    except Exception as e:
        return render_template('fidb.html', fidb=False, error=True, message=e)

# delete voice sample page


@app.route('/delete_audio_file/<int:file_id>')
def delete_audio_file(file_id):
    con = Connection()
    cur = con.cursor(buffered=True)
    cur.execute(("select audio_name from audio_info where id = %s"), (file_id,))
    for row in cur:
        file_name = row[0]
    furl = "static/uploads/"
    os.remove(furl+file_name)
    query = "DELETE FROM audio_info WHERE id = %s"
    cur.execute(query, (file_id,))
    cur.execute("select * from audio_info order by audio_name")
    return redirect(url_for('files_in_database'))

# test_voice page


@app.route('/test_voice', methods=['POST'])
def test_voice():
    print(request.method)
    if(request.method == 'POST'):
        try:
            file_name = request.form.get('test_voice_file')
            command = 'Rscript'
            path2script = 'voice/audio.R'
            print("Calling R script")
            args = [file_name]

            # Checking Audio Frequency
            furl = "static/uploads/"
            src = os.path.join(furl, file_name)
            spf = wave.open(src, "r")
            fs = spf.getframerate()
            '''
            signal = spf.readframes(-1)
            signal = np.fromstring(signal, "Int16")
            print("Signal:\n",signal)
            channels = [[] for channel in range(spf.getnchannels())]
            for index, datum in enumerate(signal):
                channels[index%len(channels)].append(datum)
            print("Channels:",channels)
            print("Frame Rate:",fs)         
            if spf.getnchannels() == 2:
                print("Just mono files")
            else:
                signal = spf.readframes(-1)
                signal = np.fromstring(signal, "Int16")
                print("Signal:\n",signal)
            '''
            # End Calculations

            cmd = [command, path2script] + args
            x = subprocess.check_output(cmd, universal_newlines=True)
            # print(x)
            print("Finished R-script execution")
            print("\n Testing..")
            res = calculate_voice(
                'voice-3000.csv', 'static/uploads/testData.csv')
            print("\n Tested Successfully. Displaying Results..")
            res['frame_rate'] = fs
            return res
        except Exception as e:
            return e


@app.route('/about.html')
def aboutpage():
    return render_template('about.html')


@app.route('/uploadfiles', methods=['POST'])
def uploadfile():
    print("Entering If loop:")
    if(request.method == 'POST'):
        print("Inside If loop")
        try:
            con = Connection()
            cur = con.cursor(buffered=True)
            fileData = request.files['audio_file']
            fname, ftype = fileData.filename, fileData.mimetype
            print("Filename:", fname, "Filetype:", ftype)
            cur.execute("select * from audio_info where audio_name = %s",
                        (os.path.splitext(fname)[0]+".wav",))
            if(cur.rowcount > 0):
                print("Error: File Exists")
                message = '<div class="alert alert-danger alert-dismissible fade show rounded" role="alert"><b style="color: red;">Message: </b>'+" File Exists!" + \
                    '<button type="button" class="close" data-dismiss="alert" aria-label="Close"><span aria-hidden="true">&times;</span></button></div>'
                return message
            else:
                furl = "static/uploads/"
                fileData.save(os.path.join(furl, fname))
                if(ftype != "audio/wav"):
                    src = os.path.join(furl, fname)
                    print("Source path: ", src)
                    fname = os.path.splitext(fname)[0]+".wav"
                    sound = AudioSegment.from_mp3(src)
                    sound.export(furl+fname, format="wav")
                    ftype = "audio/wav"
                    print("After Converted:\nFilename:",
                          fname, "Filetype:", ftype)

                size = os.stat('static/uploads/'+fname).st_size
                if(size >= 1048576):
                    length = str(round((size/1048576), 2)) + 'MB'
                if(size >= 1024):
                    length = str(round((size/1024), 2)) + 'KB'

                cur.execute("INSERT INTO audio_info(audio_name,audio_size,audio_type,audio_url) values (%s,%s,%s,%s)",
                            (fname, length, ftype, furl+fname))
                con.commit()
                print("uploaded Successfully")
                message = '<div class="alert alert-success alert-dismissible fade show rounded" role="alert"><b style="color: green;">Message: </b>' + \
                    " Uploaded Successfully"+'<button type="button" class="close" data-dismiss="alert" aria-label="Close"><span aria-hidden="true">&times;</span></button></div>'
                return message
        except Exception as e:
            con.rollback()
            print("Error:", e)
            message = '<div class="alert alert-danger alert-dismissible fade show rounded" role="alert"><b style="color: red;">Message: </b>'+e + \
                '<button type="button" class="close" data-dismiss="alert" aria-label="Close"><span aria-hidden="true">&times;</span></button></div>'
            return message
        finally:
            cur.close()
            con.close()


@app.route('/traintest', methods=['POST'])
def traintest():
    if request.method == "POST":
        csvfile = request.files['csv_file']
        print("Read CSV file")
        algorithms_name, packages, models, train_size, test_size, graph_data = Calculate(
            csvfile)
        result = dict()
        result['algorithms'] = algorithms_name
        result['packages'] = packages
        result['models'] = models
        result['train_size'] = train_size
        result['test_size'] = test_size
        result['graph_data'] = graph_data
        return result


if __name__ == "__main__":
    app.debug = True
    app.run()