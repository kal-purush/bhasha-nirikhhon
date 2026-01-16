import os
import sys
import json
import time
import asyncio

from capture_analyser import CaptureAnalyser
from Gamepad import Gamepad


base_dir = os.path.dirname(os.path.abspath(__file__))
if os.path.basename(os.getcwd()) == 'switch-control':
	root_dir = os.path.abspath(os.path.join(base_dir, "..", "switch-control"))
else:
	root_dir = os.path.abspath(os.path.join(base_dir, "../../../switch-control"))

sys.path.append(root_dir)

# Script Variables
assets = os.path.join(root_dir, "assets", "BDSP", "egghatcher")
text_bounds = (398, 884, 1141, 163)
stat_bounds = (1345, 213, 161, 510)
stat_value_bounds = [
	(1506, 211, 414, 57),
	(1506, 268, 414, 57),
	(1506, 325, 414, 57),
	(1506, 382, 414, 57),
	(1506, 439, 414, 57),
	(1506, 496, 414, 57),
	(1506, 553, 414, 57),
	(1506, 610, 414, 57)
]
gender_bounds = (1400, 21, 445, 59)
shiny_bounds = (1851, 157, 50, 51)


async def check():
	CaptureAnalyser.wait_for_text("Oh?", 1800, text_bounds)


async def walk():
	while True:
		Gamepad.press_dpad(12.5, "S")
		Gamepad.press_dpad(12.5, "N")


def check_stats(mon_stats):
	if CaptureAnalyser.look_for_image(os.path.join(assets, "stats.jpg"), 0.8, stat_bounds):
		# Check IVs
		for i, stat in stat_value_bounds:
			mon_stats[i] = CaptureAnalyser.read_text(stat)

		# Check Gender
		if CaptureAnalyser.look_for_image(os.path.join(assets, "male.jpg"), 0.8, gender_bounds):
			mon_stats[8] = "Male"
		elif CaptureAnalyser.look_for_image(os.path.join(assets, "female.jpg"), 0.8, gender_bounds):
			mon_stats[8] = "Female"
		else:
			print("Gender can't be determined")
			return False

		# Check Shiny
		if CaptureAnalyser.look_for_image(os.path.join(assets, "shiny.jpg"), 0.8, stat_bounds):
			mon_stats[9] = "Yes"
		else:
			mon_stats[9] = "None"

	else:
		print("View Stats not on correct selection")
		return False

	return mon_stats


def compare_stats(mon_stats, desired_stats):
	if len(mon_stats) != len(desired_stats):
		print("Error Processing Stats")
		return False

	for mon_value, desired_value in zip(mon_stats, desired_stats):
		if mon_value is not None and mon_value != desired_value:
			print("Mon doesn't meet desired stats")
			return False

	return True


def program(settings):
	# JSON Settings to Variables [Setting name from JSON, Default Value]
	desired_stats = [
		settings.get("HP", "None"),
		settings.get("Attack", "None"),
		settings.get("Defense", "None"),
		settings.get("Sp. Atk", "None"),
		settings.get("Sp. Def", "None"),
		settings.get("Speed", "None"),
		settings.get("Nature", "None"),
		settings.get("Ability", "None"),
		settings.get("Gender", "None"),
		settings.get("Shiny", "None"),
	]

	# Put Script Code Here
	hatch_counter = 0

	# Position Player
	Gamepad.move_left_stick(5, "SW")
	for i in range(11):
		Gamepad.press_dpad(0.1, "E")
		time.sleep(0.2)

	# Equip Bike
	Gamepad.press_button(0.1, "Plus")
	Gamepad.press_dpad(0.1, "S")

	# Walk and wait for eggs to hatch
	while hatch_counter <= 5:
		hatch_check = asyncio.create_task(check())
		walking = asyncio.create_task(walk())

		if hatch_check:
			print("Egg Hatched")
			walking.cancel()
			for i in range(3):
				Gamepad.press_button(0.1, "A")
				time.sleep(0.2)
			hatch_counter += 1

	Gamepad.press_button(0.1, "X")
	Gamepad.press_button(0.1, "A")
	Gamepad.press_button(0.1, "R")
	Gamepad.press_dpad(0.1, "W")
	Gamepad.press_dpad(0.1, "S")

	for i in range(5):
		# Determine Mon Stats
		mon_stats = []
		mon_stats = check_stats(mon_stats)
		# Keep or Release Mon
		if compare_stats(mon_stats, desired_stats):
			return True
		else:
			Gamepad.press_button(0.1, "A")
			time.sleep(0.25)
			Gamepad.press_dpad(0.1, "N")
			Gamepad.press_dpad(0.1, "N")
			Gamepad.press_dpad(0.1, "A")
			time.sleep(0.25)
			Gamepad.press_dpad(0.1, "N")
			Gamepad.press_button(0.1, "A")
			time.sleep(0.25)
			Gamepad.press_button(0.1, "A")

	return False  # End Script (True = Finished, False = Error)


def parse_args(args):
	settings = {}
	for arg in args:
		if "=" in arg:
			key, value = arg.split("=", 1)
			settings[key] = value
	return settings


def main():
	# Retrieve settings
	settings = parse_args(sys.argv[1:])
	for key, value in settings.items():
		globals()[key] = value

	print("Program Settings:")
	for key in settings:
		print(f"{key} =  {globals().get(key)}")

	# Run program and determine handle failure/success
	status = program(settings)
	status_message = "Finished" if status else "Error"

	status_file_path = os.path.abspath(os.path.join(root_dir, "../data/status.json"))
	if os.path.exists(status_file_path):
		print(f"Modifying JSON file: {status_file_path}")
		with open(status_file_path) as status_file:
			status_data = json.load(status_file)
	else:
		status_data = {}

	status_data['status'] = status_message

	# Write to JSON
	try:
		print(f"writing status data to {status_file_path}")
		with open(status_file_path, "w") as status_file:
			json.dump(status_data, status_file, indent=2)
	except Exception as e:
		print(e)

	sys.exit(0 if status else 1)


if __name__ == "__main__":
	main()