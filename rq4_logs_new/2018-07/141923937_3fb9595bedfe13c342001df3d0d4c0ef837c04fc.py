from flask_restful import Resource
from models import User, Project, Mentor, Appointment

#   /api/appointments

class Appointments:
    #   gets all active appointments
    def get(self):
        return Appointment.query.all()

#   /api/appointments/:userId
class userAppointments:

    #gets all the appintments from the specified user
    def get(self, userId):
        return Appointment
    
    #   creates a new Appointment for the user specified
    def post(self, userId):
        return Appointment

#   /api/appointments/:mentorId
class mentorAppointments:

    #   gets all appointments with the specified mentor
    def get(self, mentorId):
        return Appointment

#   /api/appointments/:appointmentId
class detailedAppointment:
    #retrieve appointment details
    def get(self, appointmentId):
        return Appointment

    #   updates the specified appointment 
    #   (should be used after choosing mentor to assign mentor)
    def put(self, appointmentId):
        return Appointment
    
    #   cancel an appointment
    def delete(self, appointmentId):
        return Appointment