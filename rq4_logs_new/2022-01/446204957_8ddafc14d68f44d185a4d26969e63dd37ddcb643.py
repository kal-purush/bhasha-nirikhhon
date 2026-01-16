# prog to check email validation

import re

email_pattern = '^[\w|.]+@\w+\.[a-zA-Z]+'
emailid = "first.second@gmail.com"          # valid
# emailid = "@gmail.com"                    # invalid

if re.match(email_pattern,emailid):
    print("valid")

else:
    print("invalid")