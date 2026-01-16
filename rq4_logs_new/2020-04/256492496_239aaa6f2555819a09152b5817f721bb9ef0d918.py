import firebase_admin
from firebase_admin import auth, credentials

cred = credentials.Certificate("firebase-admin-secretkey.json")
firebase_admin.initialize_app(cred)

EmailList = ["abc@email.com"]

try:
    for email in EmailList:
        try:
            user = auth.get_user_by_email(email)
            auth.delete_user(user.uid)
            print('Successfully deleted user')
        except Exception:
            print("User retreival/removal error")
            continue
except Exception:
    print("Exception Occurred")