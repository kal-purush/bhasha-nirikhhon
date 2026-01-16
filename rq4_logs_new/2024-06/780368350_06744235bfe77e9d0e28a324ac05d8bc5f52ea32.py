import keyboard
def k_listener(expected_key):
    # fuck_you = keyboard.read_event()
    # if fuck_you.event_type == keyboard.KEY_DOWN:
    #     # print('key is down')
    #     return fuck_you.name
    return keyboard.read_event().name

print(k_listener('s'))