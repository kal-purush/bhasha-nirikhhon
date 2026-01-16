from flask_restful import Resource
from flask_restful import reqparse
from flask_restful import fields, marshal_with
from .models import Caretaker,User,followers,UserPatient
from application.validation import BusinessValidationError
from application.database import db
from datetime import datetime
from main import app as app
from flask_security import auth_required
from sqlalchemy.sql import text
from werkzeug.security import generate_password_hash
import os

response_fields = {

    "username" : fields.String,
   
}
  
'''
Request Parsers and Output Resource Fields for UserPatientAPI 
'''
user_parser = reqparse.RequestParser()
user_parser.add_argument("user_id")
user_parser.add_argument("email")
user_parser.add_argument("dob")
user_parser.add_argument("location")
user_parser.add_argument("phone")
user_parser.add_argument("password")

class UserPatientAPI(Resource):
    
    @marshal_with(response_fields)
    @auth_required("token")
    def get(self,user_id=None):
        # Default to 200 OK
        if user_id!=None:
            data=UserPatient.query.with_entities(UserPatient.user_id).filter_by(user_id=user_id).all()
            return data,200

        
    @auth_required("token")
    @marshal_with(response_fields)
    def post(self):
        args = user_parser.parse_args() 
        user_id=args.get("user_id", None)
        username=args.get("username", None)
        dob=args.get("dob", None)
        location=args.get("location", None)
        phone=args.get("phone", None)
        password=args.get("password", None)
        user = db.session.query(UserPatient).filter_by(user_id=user_id).first()   
        if user is None:
            raise BusinessValidationError(status_code=404, error_code="ERROR_U00", error_message="User not found")        
        
        add_user = UserPatient(user_id=user_id,username=username,dob=dob,location=location,phone=phone,password=password)
        db.session.add(add_user)
        db.session.commit()

        Msg={"message":"User Added"}
        return Msg,200 

    @auth_required("token")
    @marshal_with(response_fields)
    def delete(self, user_id):

        exists=db.session.query(UserPatient).filter_by(user_id=user_id).first() 
        if exists is None:
            raise BusinessValidationError(status_code=400, error_code="ERROR_U03", error_message="You have not liked the post")
        db.session.delete(exists)
        db.session.commit()
        Msg={"message":"User Deleted"}
        return Msg,200 
    

    # put for appending the caretaker info 
    
'''
Request Parsers and Output Resource Fields for CaretakerAPI 
'''

caretaker_parser = reqparse.RequestParser()

caretaker_parser.add_argument("caretaker_id" )
caretaker_parser.add_argument("name")
caretaker_parser.add_argument("mobile")
caretaker_parser.add_argument("caretaker_pic")
caretaker_parser.add_argument("description")


blog_fields = {
    "caretaker_id": fields.String,
    "name": fields.String,
    "mobile": fields.String,
    "caretaker_pic":fields.String,
    "description":  fields.String,
}

blog_resource_fields = {

    "username": fields.String,
    "id": fields.Integer,
    "blogs": fields.List(fields.Nested(blog_fields))
}
class CaretakerAPI(Resource):

    
    @marshal_with(blog_resource_fields)
    def get(self,user_id=None,blog_id=None):
        # Default to 200 OK
        if blog_id!=None:
            blog = Caretaker.query.filter_by(blog_id=blog_id).first()   
            if blog is None:
                raise BusinessValidationError(status_code=404, error_code="ERROR_B01", error_message="Blog not found")
            else:
                resp={"blogs":blog,"id":user_id}
                return resp,200
        if user_id!=None:
            data = Caretaker.query.filter_by(id=user_id).first()   
            if data is None:
                raise BusinessValidationError(status_code=404, error_code="ERROR_U00", error_message="User not found")
                  
            else:
                return data,200
        
    
    @auth_required("token")
    @marshal_with(blog_fields)
    def post(self):
        args = caretaker_parser.parse_args() 
        caretaker_id=args.get("caretaker_id",None)
        name=args.get("name", None)
        mobile=args.get("mobile", None)
        caretaker_pic=args.get("caretaker_pic", None)
        description=args.get("description", None)
        
        if name is None:
            raise BusinessValidationError(status_code=400, error_code="ERROR_B02", error_message="Caretaker Name is required")

        caretaker = Caretaker(caretaker_id=caretaker_id,name=name,mobile=mobile,caretaker_pic=caretaker_pic,description=description)
        caretaker = Caretaker.query.filter_by(name=name).first() 
        
        if caretaker!=None:
            raise BusinessValidationError(status_code=400, error_code="ERROR_B04", error_message="Caretaker Name already exists.. Please choose another name")
        else:
            db.session.add(caretaker)              
            db.session.commit()
        
        return caretaker,200

    @auth_required("token")
    @marshal_with(response_fields)
    def delete(self,user_id=None,blog_id=None):
        # Default to 200 OK
        if blog_id!=None:
            blog = Blog.query.filter_by(blog_id=blog_id).first()   
          
            if blog is None:
                raise BusinessValidationError(status_code=404, error_code="ERROR_B01", error_message="Blog not found")
            else:
                data = User.query.filter_by(id=user_id).first()   
                if data is None:
                        raise BusinessValidationError(status_code=404, error_code="ERROR_B01", error_message="Blog not found")       
                else:
                    '''likes=Likes.query.filter_by(blog_id=blog_id).all()
                    for like in likes:
                        print(like)
                        db.session.delete(like)
                        db.session.commit()   '''
                    try:
                        os.remove(app.config["UPLOAD_FOLDER"]+'/'+blog.image_loc)
                    except:
                        pass
                    db.session.delete(blog)
                    data.no_blogs-=1
                    db.session.commit()           
        
        Msg={"message":"Successfully Deleted"}
        return Msg,200 
            
'''
Request Parsers and Output Resource Fields for UserAPI 
'''

user_parser = reqparse.RequestParser()

user_parser.add_argument("id", location='form')
user_parser.add_argument("username", location='form')
user_parser.add_argument("password", location='form')
user_parser.add_argument("email", location='form')

user_resource_fields = {
    "id": fields.Integer,
    "username": fields.String,
    "email": fields.String,
    "profile_pic": fields.String,
    "no_blogs": fields.Integer,
    "no_followers": fields.Integer,
    "no_following": fields.Integer,
    "blogs": fields.List(fields.Nested(blog_fields))
    
}

class TasksAPI(Resource):

    @auth_required("token")
    @marshal_with(user_resource_fields)
    def get(self,user_id=None):
        # Default to 200 OK
        if user_id!=None:
            data = User.query.filter_by(id=user_id).first()   
            if data is None:
                raise BusinessValidationError(status_code=404, error_code="ERROR_U00", error_message="User not found")
                  
            else:
                return data,200
    
    @auth_required("token")
    @marshal_with(response_fields)
    def put(self):
        args = user_parser.parse_args() 
        username=args.get("username", None)
        email=args.get("email", None)
        password=args.get("password", None)
        user_id=args.get("id",None)
        #print(user_id,username,email,password)
        data = User.query.filter_by(id=user_id).first()   
        if data is None:
                raise BusinessValidationError(status_code=400, error_code="ERROR_U01", error_message="Email address already taken")
        else:
            data.username=username
            data.email=email
            data.password=password
            db.session.commit()

        Msg={"message":"User updated successfully"}
        return Msg,200 
    
    @marshal_with(response_fields)
    def post(self):
        args = user_parser.parse_args() 
        username=args.get("username", None)
        email=args.get("email", None)
        password=args.get("password", None)

        if app.security.datastore.find_user(email=email):
            raise BusinessValidationError(status_code=400, error_code="ERROR_U01", error_message="Email address already taken")
        elif app.security.datastore.find_user(username=username):
            raise BusinessValidationError(status_code=400, error_code="ERROR_U02", error_message="Username already taken")
        else:
            app.security.datastore.create_user(username=username,email=email,password=password,fs_uniquifier=generate_password_hash(password))
            db.session.commit()
        Msg={"message":"User created successfully"}
        return Msg,200 


    @auth_required("token")
    @marshal_with(response_fields)
    def delete(self,user_id=None):
        # Default to 200 OK
        if user_id!=None:
            data = User.query.filter_by(id=user_id).first()   
            if data is None:
                raise BusinessValidationError(status_code=404, error_code="ERROR_U00", error_message="User not found")    
            else:
                db.session.delete(data)
                db.session.commit()      
        Msg={"message":"User deleted successfully"}
        return Msg,200 

            
'''
Request Parsers and Output Resource Fields for FollowerAPI 
'''

follower_parser = reqparse.RequestParser()

follower_parser.add_argument("user_id")
follower_parser.add_argument("follow_id")

follower_resource_fields = {
    "follow_id": fields.Integer
}

class ContactsAPI(Resource):
    
    @marshal_with(follower_resource_fields)
    @auth_required("token")
    def get(self,user_id=None):
        # Default to 200 OK
        if user_id!=None:
            data = User.query.filter_by(id=user_id).first()   
            if data is None:
                raise BusinessValidationError(status_code=404, error_code="ERROR_U00", error_message="User not found")
                  
            else:
                data=db.session.query(followers).filter_by(user_id=user_id).all()
                #print(data)
                return data,200
        
    @auth_required("token")
    @marshal_with(response_fields)
    def post(self):
        args = follower_parser.parse_args() 
        user_id=args.get("user_id", None)
        follow_id=args.get("follow_id", None)
        #print(user_id,follow_id)
        user = db.session.query(User).filter_by(id=user_id).first()   
        if user is None:
            raise BusinessValidationError(status_code=404, error_code="ERROR_U00", error_message="User not found")
        follow = db.session.query(User).filter_by(id=follow_id).first()   
        if follow is None:
            raise BusinessValidationError(status_code=404, error_code="ERROR_U00", error_message="User not found")
        exists=db.session.query(followers).filter_by(user_id=user_id,follow_id=follow_id).first() is not None
        if exists is None:
            raise BusinessValidationError(status_code=400, error_code="ERROR_U03", error_message="You are already following user")
        
        user.followers.append(follow)
        #add_follower = Followers(user_id=user_id,follower_id=follow_id)
        #db.session.add(add_follower)
        user.no_following+=1  
        follow.no_followers+=1
        db.session.commit()

        Msg={"message":"Follower addeed successfully"}
        return Msg,200 

    @auth_required("token")
    @marshal_with(response_fields)
    def delete(self,user_id,follow_id):

        user = db.session.query(User).filter_by(id=user_id).first()   
        if user is None:
            raise BusinessValidationError(status_code=404, error_code="ERROR_U00", error_message="User not found")
        follow = db.session.query(User).filter_by(id=follow_id).first()   
        if follow is None:
            raise BusinessValidationError(status_code=404, error_code="ERROR_U00", error_message="User not found")
        exists=db.session.query(followers).filter_by(user_id=user_id,follow_id=follow_id).first() 
        if exists is  None:
            raise BusinessValidationError(status_code=400, error_code="ERROR_U03", error_message="You are not following user")
        
        query = text("delete from followers where followers.user_id="+str(user_id)+" and followers.follow_id="+str(follow_id))
    
        db.session.execute(query)
       
        user.no_following-=1  
        follow.no_followers-=1
        db.session.commit()      
        Msg={"message":"User Unfollowed"}
        return Msg,200 
    