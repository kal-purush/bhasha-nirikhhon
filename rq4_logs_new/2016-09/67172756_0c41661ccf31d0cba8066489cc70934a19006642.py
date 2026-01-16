from flask import flash, redirect, jsonify, session, url_for
from src import app, db
from src.models import Comment


@app.route('/')
def api_comments():
    results = db.session.query(Comment).limit(10).offset(0).all()
    json_results = []
    for result in results:
        data = {
            'comment_id': result.comment_id,
            'comment': result.coment,
            'author': result.author,
            'posted_date': result.posted_date
        }
        json_results.append(data)
    return jsonify(items=json_results)