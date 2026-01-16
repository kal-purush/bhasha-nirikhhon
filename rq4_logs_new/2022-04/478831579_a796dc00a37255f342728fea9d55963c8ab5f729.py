
try:
  import usocket as socket
except:
  import socket
import uos, machine
#uos.dupterm(None, 1) # disable REPL on UART(0)
import gc
import webrepl
import network
import esp

esp.osdebug(None)
gc.collect()

ssid = 'Bella'
password = 'BellaGatoSemVergonha'


nic = network.WLAN(network.STA_IF)
nic.active(True)
nic.connect('Bella', 'BellaGatoSemVergonha')

webrepl.start()