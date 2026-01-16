from supabase import Client
from typing import Dict
import re
from utils.db_operations import insert_profile_to_supabase


def validate_password(password: str) -> bool:
    if len(password) < 8:
        print("Password must be at least 8 characters long.")
        return False
    if not re.search(r"\d", password):
        print("Password must contain at least one number.")
        return False
    if not re.search(r"[A-Z]", password):
        print("Password must contain at least one uppercase letter.")
        return False
    if not re.search(r"[a-z]", password):
        print("Password must contain at least one lowercase letter.")
        return False
    if not re.search(r"[!@#$%^&*(),.?\":{}|<>]", password): 
        print("Password must contain at least one special character.")
        return False
    return True
#added supabase class to function params and changed the return to a Dict so it can 
#return the response from supabase or error message
def register_user(email: str, password: str, name: str, supabase:Client) -> Dict[str, any]:
    try:
        if not validate_password(password):
            print("Password does not meet requirements.")
            return {
                "success": False,
                "error": "Password does not meet requirements."}
        response = supabase.auth.sign_up({"email": email, "password": password})
        if response.user:
            print(f"User {response.user.email} registered successfully.")
            user_id = response.user.id
            # Insert user profile into the database
            profile_data = {
                "id": user_id,
                "name": name,
                "email": email
            }
            insert_response = insert_profile_to_supabase(profile_data, supabase)
            if insert_response.get("success"):
                print(f"Profile for {name} inserted successfully.")
            else:
                print(f"Failed to insert profile for {name}: {insert_response.error}")
                return {
                    "success": False,
                    "error": (f"Failed to insert profile for {name}: {insert_response.error}. This is probably really bad because the user is registered with supabase but not in the database.")}
                
            return {
                "success": True,
                "data": response
            }
        else:
            print(f"Registration failed")
            return {
                "success": False,
                "error": "Unable to register user."}
    except Exception as e:
        print(f"Error during registration: {e}")
        return {
            "success": False,
            "error": str(e)
        }


def login_user(email: str, password: str, supabase:Client) -> bool:
    try:
        response = supabase.auth.sign_in_with_password({"email": email, "password": password})
        if response.session:
            print("Logged in successfully.")
            return {
                "success": True,
                "data": response}
        else:
            print(f"Login failed")
            return {
                "success": False,
                "error": "Unable to log in user."}
            
    except Exception as e:
        print(f"Error during login: {e}")
        return {
            "success": False,
            "error": str(e)
        }
    