from m5stack import *
from m5ui import *
from uiflow import *
import time


setScreenColor(0x222222)


battery_level = None
is_charging = None



body = M5Rect(60, 37, 191, 108, 0x222222, 0xFFFFFF)
percentage = M5TextBox(115, 179, "100", lcd.FONT_DejaVu40, 0xFFFFFF, rotate=0)
head = M5Rect(252, 60, 18, 60, 0xFFFFFF, 0xFFFFFF)
batt_1 = M5Rect(68, 44, 38, 93, 0xFFFFFF, 0xFFFFFF)
batt_2 = M5Rect(114, 44, 38, 93, 0xFFFFFF, 0xFFFFFF)
batt_3 = M5Rect(159, 44, 38, 93, 0xFFFFFF, 0xFFFFFF)
batt_4 = M5Rect(205, 44, 38, 93, 0xFFFFFF, 0xFFFFFF)




lcd.setBrightness(10)
while True:
  battery_level = power.getBatteryLevel()
  is_charging = power.isCharging()
  if battery_level < 100:
    percentage.setPosition(x=130)
  else:
    percentage.setPosition(x=115)
  percentage.setText(str(battery_level))
  if is_charging:
    batt_1.hide()
    batt_2.hide()
    batt_3.hide()
    batt_4.hide()
    wait(0.5)
    batt_1.show()
    wait(0.5)
    batt_2.show()
    wait(0.5)
    batt_3.show()
    wait(0.5)
    batt_4.show()
    wait(0.5)
  else:
    if battery_level >= 25:
      batt_1.show()
    else:
      batt_1.hide()
    if battery_level >= 50:
      batt_2.show()
    else:
      batt_2.hide()
    if battery_level >= 75:
      batt_3.show()
    else:
      batt_3.hide()
    if battery_level >= 100:
      batt_4.show()
    else:
      batt_4.hide()
  wait_ms(2)