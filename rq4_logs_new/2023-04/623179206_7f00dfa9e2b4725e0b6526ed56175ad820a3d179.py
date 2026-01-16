import time


def calculate_go_home():
    hour_now = int(time.strftime("%H"))
    limit_hour = 7
    if hour_now > limit_hour:
        print("Is hour of go home")
    else:
        difference = str(limit_hour - hour_now)
        print(difference + " hours left to go home")


calculate_go_home()