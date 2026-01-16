expected_value = bytes(b'\x04\x09\x0c\xb0')
expected_value_int_big = int.from_bytes(expected_value, "big")
expected_value_int_little = int.from_bytes(expected_value, "little")
time_to_check_millis = 1610910897448
time_to_check_seconds = int(time_to_check_millis / 1000)

print('expected value: {}'.format(expected_value))
print('expected value int big: {}'.format(expected_value_int_big))
print('expected value int little: {}'.format(expected_value_int_little))
print('time_to_check_seconds: {}'.format(time_to_check_seconds))
print('time_to_check_millis: {}'.format(time_to_check_millis))

currentTimeMillis = time_to_check_seconds % 32768
i = currentTimeMillis * 31
to_add = 0

for extra in range(0, 50):
    val = (i + extra) * 31
    print('found val: {}'.format(val))
    print('val bytes: {}'.format(val.to_bytes(4, 'little')))
    print()