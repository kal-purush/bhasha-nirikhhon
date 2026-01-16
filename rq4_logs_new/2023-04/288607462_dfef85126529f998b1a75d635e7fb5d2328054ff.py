import time

def two_digit_int_as_7_seg(x: int) -> str:
    a = "🯰🯱🯲🯳🯴🯵🯶🯷🯸🯹"
    idx_0 = (x//10)%10
    return a[idx_0] + a[x%10]


def time_as_7_seg_display():
    t = time.localtime()
    hour_and_mins = (two_digit_int_as_7_seg(t.tm_hour), two_digit_int_as_7_seg(t.tm_min))
    return ':'.join(hour_and_mins)

ret_val = time_as_7_seg_display()
print(ret_val)