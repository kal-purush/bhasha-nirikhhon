from flask import Flask,Blueprint,jsonify,make_response

meetup_api = Blueprint('meetup_api',__name__,)

@meetup_api.route('/meetups/upcoming',methods=["GET"])
def upcomingMeetup():
    return make_response(jsonify({"message":"upcoming meetup"}),200)

@meetup_api.route('/meetups/<string:id>',methods=["GET"])
def getSpecificmeetup(id):
    return make_response(jsonify({"message":"get a specific meetup"}),200)

@meetup_api.route('/meetups/<string:id>/rsvps',methods=["POST"])
def respondMeetuprsvp(id):
    return make_response(jsonify({"message":"PLEASE RSVP TO A MEET UP"}),201)