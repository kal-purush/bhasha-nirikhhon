# coding=utf-8
from __future__ import absolute_import
import octoprint.plugin
import serial
import binascii
from threading import Thread
import time
import RPi.GPIO as GPIO

from time import sleep


class OctokeysPlugin(octoprint.plugin.SettingsPlugin,
					   octoprint.plugin.AssetPlugin,
					   octoprint.plugin.TemplatePlugin,
					   octoprint.plugin.StartupPlugin,
					   octoprint.plugin.ShutdownPlugin):
	##~~ SettingsPlugin mixin

	def get_settings_defaults(self):
		return dict(
			comport="COM3",
			baudrate=115200,
			baudrateOpt=[9600, 19200, 115200],
			
			userCommand1="",
			userCommand2="",
			userCommand3="",
			userCommand4="",
			userCommand5="",
			userCommand6="",
			
			userKeyMode1=0,
			userKeyMode2=0,
			userKeyMode3=0,
			userKeyMode4=0,
			userKeyMode5=0,
			userKeyMode6=0,
			
			userCommands=["", "", "", "", "", ""],
			userKeyModes=[0, 0, 0, 0, 0, 0],  # 0 -> OFF, 1 -> GCODE, 2 -> Script, 3 -> Action
			userKeyModeOptions=["OFF", "GCODE", "SCRIPT", "ACTION"]

		)

	def get_config_vars(self):
		return dict(
			comport=self._settings.get(["comport"]),
			baudrate=self._settings.get(["baudrate"]),
			baudrateOpt=self._settings.get(["baudrateOpt"]),
			
			userCommands=[ self._settings.get(["userCommand1"]),self._settings.get(["userCommand2"]),
						   self._settings.get(["userCommand3"]),self._settings.get(["userCommand4"]),
						   self._settings.get(["userCommand5"]),self._settings.get(["userCommand6"])],

			userKeyModes=[ self._settings.get(["userKeyMode1"]),self._settings.get(["userKeyMode2"])
						 , self._settings.get(["userKeyMode3"]),self._settings.get(["userKeyMode4"])
						 , self._settings.get(["userKeyMode5"]),self._settings.get(["userKeyMode6"])],

		)

	def get_template_configs(self):
		return [
			# dict(type="navbar", custom_bindings=False),
			dict(type="settings", custom_bindings=False)
		]

	def on_settings_save(self, data):
		octoprint.plugin.SettingsPlugin.on_settings_save(self, data)
		self.stop_com_thread()
		self.start_com_thread()

	# restart the thread
	##~~ AssetPlugin mixin


	def get_assets(self):
		# Define your plugin's asset files to automatically include in the
		# core UI here.
		return dict(
			js=["js/OctoKeys.js"],
			css=["css/OctoKeys.css"],
			less=["less/OctoKeys.less"]
		)

	##~~ Softwareupdate hook

	def get_update_information(self):
		# Define the configuration for your plugin to use with the Software Update
		# Plugin here. See https://github.com/foosel/OctoPrint/wiki/Plugin:-Software-Update
		# for details.
		return dict(
			Octokeys=dict(
				displayName="OctoKeys Plugin",
				displayVersion=self._plugin_version,

				# version check: github repository
				# Здесь должна быть ссылка на репозиторий с для обновления плагина сейчас все ссылки не корректны
				type="github_release",
				user="pkElectronics",
				repo="OctoPrint-Octokeys",
				current=self._plugin_version,

				# update method: pip
				pip="https://github.com/pkElectronics/OctoPrint-Octoremote/archive/{target_version}.zip"
			)
		)

	def on_after_startup(self):
		self.start_com_thread()
		GPIO.setmode(GPIO.BCM)

	def start_com_thread(self):
		conf = self.get_config_vars()
		self.comthread = SerialThread(self, conf)

	# self.comthread.start()

	def stop_com_thread(self):
		self.comthread.interrupt()
		self.comthread.join()

	def on_shutdown(self):
		self._logger.info("on shutdown")
		GPIO.cleanup()
		self.comthread.interrupted = True
		self.comthread.interrupt()

	# Other stuff below
	#
	#
	def getPrinterObject(self):
		return self._printer

	def getLogger(self):
		return self._logger

	# serial.tools.list_ports listet alle comports auf


# If you want your plugin to be registered within OctoPrint under a different name than what you defined in setup.py
# ("OctoPrint-PluginSkeleton"), you may define that here. Same goes for the other metadata derived from setup.py that
# can be overwritten via __plugin_xyz__ control properties. See the documentation for that.
__plugin_name__ = "OctoKeys Plugin"


def __plugin_load__():
	global __plugin_implementation__
	__plugin_implementation__ = OctokeysPlugin()

	global __plugin_hooks__
	__plugin_hooks__ = {
		"octoprint.plugin.softwareupdate.check_config": __plugin_implementation__.get_update_information
	}


class SerialThread(Thread):
	# Fixed responses
	ackResponse = bytearray([0x80, 0x07, 0x01, 0xC3, 0x64, 0x32, 0x26])  # 263264C3
	nackResponse = bytearray([0x80, 0x07, 0x02, 0x79, 0x35, 0x3B, 0xBF])  # BF3B3579

	# comport parameters
	portname = ""
	baudrate = 9600

	# thread parameters
	interrupted = False

	# msg parser vars
	msgParsingState = 0
	bytesRead = []
	payload = []
	countBytesRead = 0
	ackPending = False


	def __init__(self, callbackClass, config):
		Thread.__init__(self)
		self.cbClass = callbackClass
		self.portname = config["comport"]
		self.baudrate = config["baudrate"]


		self.userCommands = config["userCommands"]
		self.userKeyModes = config["userKeyModes"]


		try:
			self.port = serial.Serial(self.portname, baudrate=self.baudrate, timeout=3.0)
		except:
			self.interrupt()
			callbackClass.getLogger().error("Octokeys, could not open comport:" + self.portname)
		callbackClass.getLogger().info("Octokeys Comthread started")
		self.daemon = False
		self.start()

	def run(self):
		self.cbClass.getLogger().info("Thread started")
		#self.sendCommandWithPayload(0x20, [self.movementIndex], 1) Изначальное включение светодиодов при запуске
		#self.sendCommandWithPayload(0x20, [self.toolIndex + 4], 1) Команда управления светодиодами, состояние светодиодов, кол-во байт данных

		while not self.interrupted:
			try:
				readbyte = self.port.read(1)
				if self.msgParsingState == 0:
					if readbyte == '\x80':
						self.bytesRead.append(ord(readbyte))
						self.msgParsingState += 1
						self.countBytesRead += 1

				elif self.msgParsingState == 1:
					self.telegramLength = ord(readbyte)
					self.bytesRead.append(ord(readbyte))
					self.msgParsingState += 1
					self.countBytesRead += 1

				elif self.msgParsingState == 2:
					self.command = ord(readbyte)
					self.bytesRead.append(ord(readbyte))
					self.msgParsingState += 1
					self.countBytesRead += 1
					if self.telegramLength == 7:
						self.msgParsingState += 1

				elif self.msgParsingState == 3:
					self.bytesRead.append(ord(readbyte))
					self.payload.append(ord(readbyte))
					self.countBytesRead += 1
					if self.countBytesRead == self.telegramLength - 4:
						self.msgParsingState += 1
				elif self.msgParsingState == 4:
					self.crc32 = ord(readbyte)
					self.countBytesRead += 1
					self.msgParsingState += 1

				elif self.msgParsingState == 5:
					self.crc32 |= ord(readbyte) << 8
					self.countBytesRead += 1
					self.msgParsingState += 1

				elif self.msgParsingState == 6:
					self.crc32 |= ord(readbyte) << 16
					self.countBytesRead += 1
					self.msgParsingState += 1

				elif self.msgParsingState == 7:
					self.crc32 |= ord(readbyte) << 24
					self.countBytesRead += 1
					self.msgParsingState += 1
					crc32 = binascii.crc32(bytearray(self.bytesRead)) % (1 << 32)
					if crc32 == self.crc32:
						self.performActions(self.command, self.payload)
					else:
						self.sendNack()

					self.msgParsingState = 0
					self.crc32 = 0
					self.countBytesRead = 0
					self.bytesRead = []
					self.payload = []
					self.telegramLength = 0
					self.command = 0
			except:
				pass
		self.port.close()

	def interrupt(self):
		self.interrupted = True

	# А вот тут мы разбираем принятый пакет
	# я использую следующие кнопки: PLAY_PAUSE 0x11, STOP 0x12, USER1 0x13, USER2 0x14, HEAT1 0x21, HEAT2 0x22, LITE 0x23, POWER 0x24
	def performActions(self, cmd, payload):
		try:
			if cmd == 0x01:
				self.ackPending = False
			elif cmd == 0x02:
				self.resendLastMessage()
			elif cmd == 0x10:  # key pressed
				self.sendAck()
				if payload[0] == 0x11:
					#PLAY_PAUSE
					if self.getPrinterObject().is_printing:
						self.getPrinterObject().pause_print()
						# Включим красный диодик если остановились и выключим зелёный
						self.sendCommandWithPayload(0x20, 2, 1)
						self.sendCommandWithPayload(0x20, 3, 1)
					else:	
						if self.getPrinterObject().is_paused():
							self.getPrinterObject().resume_print()
							# Включим зелёный диодик если печатаем и выключим красный
							self.sendCommandWithPayload(0x20, 1, 1)
							self.sendCommandWithPayload(0x20, 4, 1)
						else:
							self.getPrinterObject().start_print()
							# Включим зелёный диодик если печатаем и выключим красный
							self.sendCommandWithPayload(0x20, 1, 1)
							self.sendCommandWithPayload(0x20, 4, 1)
				elif payload[0] == 0x12:
					# STOP
					self.getPrinterObject().cancel_print()
					# Включим красный диодик если остановились и выключим зелёный
					self.sendCommandWithPayload(0x20, 0, 1)
					self.sendCommandWithPayload(0x20, 3, 1)
				elif payload[0] == 0x13:
					# USER1
					self.performUserCommandByID(0)
				elif payload[0] == 0x14:
					# USER2
					self.performUserCommandByID(1)
				elif payload[0] == 0x21:
					# HEAT1
					self.performUserCommandByID(2)
				elif payload[0] == 0x22:
					# HEAT2
					self.performUserCommandByID(3)
				elif payload[0] == 0x23:
					# LITE
					self.performUserCommandByID(4)
				elif payload[0] == 0x24:
					# POWER
					self.performUserCommandByID(5)
				elif payload[0] == 0x31:
					# Кнопка не реализована аппаратно
					pass
				elif payload[0] == 0x32:
					# Кнопка не реализована аппаратно
					pass
				elif payload[0] == 0x33:
					# Кнопка не реализована аппаратно
					pass
				elif payload[0] == 0x34:
					# Кнопка не реализована аппаратно
					pass
				elif payload[0] == 0x41:
					# Кнопка не реализована аппаратно
					pass
				elif payload[0] == 0x42:
					# Кнопка не реализована аппаратно
					pass
				elif payload[0] == 0x43:
					# Кнопка не реализована аппаратно
					pass
				elif payload[0] == 0x44:
					# Кнопка не реализована аппаратно
					pass
				elif payload[0] == 0x51:
					# Кнопка не реализована аппаратно
					pass
				elif payload[0] == 0x52:
					# Кнопка не реализована аппаратно
					pass
				elif payload[0] == 0x53:
					# Кнопка не реализована аппаратно
					pass
			elif cmd == 0x11:  # key released
				self.stuff = ""
			 self.cbClass._logger.info("KR")
			elif cmd == 0x12:  # key longpress
				self.stuff = ""
			# self.cbClass._logger.info("KL")
			else:
				self.stuff = ""
			# self.cbClass._logger.info("FAIL")
		except:
			pass

	def sendAck(self):
		try:
			self.port.write(self.ackResponse)
		except:
			pass

	def sendNack(self):
		try:
			self.port.write(self.nackResponse)
		except:
			pass

	def sendCommandWithPayload(self, cmd, payload, payloadLength):
		try:
			message = []
			message.append(0x80)
			message.append(payloadLength + 7)
			message.append(cmd)
			message = message + payload
			bytes = bytearray(message)
			crc32 = binascii.crc32(bytes) % (1 << 32)

			message.append(int(crc32 & 0xFF))
			message.append(int(crc32 >> 8 & 0xFF))
			message.append(int(crc32 >> 16 & 0xFF))
			message.append(int(crc32 >> 24 & 0xFF))
			self.lastMessage = bytearray(message)
			self.port.write(self.lastMessage)

			self.ackPending = True
		except:
			pass

	def resendLastMessage(self):
		try:
			self.port.write(self.lastMessage)
		except:
			pass

	def getPrinterObject(self):
		return self.cbClass.getPrinterObject()

	def performUserCommandByID(self, ubid):
		self.cbClass._logger.info("User Command %s", ubid)
		self.cbClass._logger.info(self.userCommands[ubid])
		self.cbClass._logger.info(self.userKeyModes[ubid])
		if self.userCommands[ubid] != "":
			if self.userKeyModes[ubid] == "GCODE":
				self.getPrinterObject().commands(self.userCommands[ubid])
			elif self.userKeyModes[ubid] == "SCRIPT":
				self.getPrinterObject().script(self.userCommands[ubid])
			elif self.userKeyModes[ubid] == "ACTION":
				GPIO.setup(int(self.userCommands[ubid]), GPIO.OUT)
				GPIO.output(int(self.userCommands[ubid]), not GPIO.input(int(self.userCommands[ubid])))
				# По идее этот код должен повторно настроить пин на выход и инвертировать его стостояние