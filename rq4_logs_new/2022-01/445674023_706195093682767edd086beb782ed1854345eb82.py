import usb_hid, gc, time
def report(report_code: list[int], rel: bool = True, slp = 0.02) -> None: # simulates a key press by sending an 8 byte long report with certain criteria
    kbd.send_report(bytearray(report_code), 1)
    time.sleep(slp)
    if rel:
        kbd.send_report(bytearray([0]*8)) # not calling recursively because of garbage collection
        time.sleep(0.001)
    gc.collect() # this takes a couple milliseconds to complete, but the USB pulling rate is 1000HZ so it doesn't make a noticable difference in the grand scheme of things

kbd = [device for device in usb_hid.devices if device.usage_page == 0x1 and device.usage == 0x6 and hasattr(device, "send_report")][0] # hehe global data go brrrrrrrrrrrrrr