import os
from flask import Flask, render_template, request, redirect, url_for, jsonify
from werkzeug.utils import secure_filename
import metrics
import numpy as np

ALLOWED_EXTENSIONS = {'tsv'}
APP_ROOT = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(APP_ROOT, 'uploads')

app = Flask(__name__)
app.secret_key = 'super secret key'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def main():
    # check if the post request has the file part
    if 'file' not in request.files:
        print('No file part')
        return redirect(request.url)
    file = request.files.get('file')
    # if user does not select file, browser also
    # submit a empty part without filename
    if not file:
        pass #todo: error

    if file.filename == '':
        print('No selected file')
        return redirect(request.url)
    if allowed_file(file.filename):
        filename = secure_filename(file.filename)
        file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
        return redirect(url_for('visualization', filename=filename))
    return redirect(url_for('index'))


@app.route('/visualization/<filename>')
def visualization(filename=None):
    if filename is None:
        return "File not found", 404

    metrics_data = metrics.Metrics(filename)
    return render_template('visualization.html', data=None,
                           participants=metrics_data.getParticipantsList(),
                           aois=metrics_data.get_aois_list(),
                           filename=filename)


@app.route('/add', methods=['POST'])
def add_visualization():
    filename = request.form['filename']
    id = request.form['block_id']
    if filename is not None:
        metrics_data = metrics.Metrics(filename)
        return render_template('visualization_block.html', data=None,
                               participants=metrics_data.getParticipantsList(),
                               aois=metrics_data.get_aois_list(),
                               filename=filename,
                               block_id=id)
    return "File not found"


@app.route('/filter', methods=['POST'])
def filter():
    participants = request.form.getlist('participant_id')
    aois = request.form.getlist('aois')
    filename = request.form['filename']
    min = request.form['min']
    max = request.form['max']
    aoi_setting = request.form['aoi_setting']

    if filename is None:
        return 'error'

    metrics_data = metrics.Metrics(filename)
    aoi_list = metrics_data.get_aois_list()
    if aoi_setting in ('all', 'filtered'):
        aois = aoi_list

    transitions_data = metrics_data.filter_transitions(participants=participants, aois=aois, min=min, max=max)
    filtered_participants = transitions_data['participants_list']
    transitions = transitions_data['transitions']

    if aoi_setting == 'filtered':
        rows = transitions.any(axis=1)
        columns = transitions.any(axis=0)
        to_delete = []
        for index in range(len(rows)):
            if rows[index] == False and columns[index] == False:
                to_delete.append(index)

        transitions = np.delete(transitions, to_delete, 1)
        transitions = np.delete(transitions, to_delete, 0)
        aois = np.delete(aoi_list, to_delete, None)
    stats = metrics_data.get_stats(participants, aois)
    data = {'stats': stats, 'transitions': transitions.tolist(), 'participants': filtered_participants, 'selected_participants': participants}
    return jsonify(data)


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


if __name__ == '__main__':
    app.run(host='0.0.0.0')