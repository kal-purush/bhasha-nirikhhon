mods = []
for i in range(100):
	mods.append([f"insane_ai_boost_{i+1} = {{"])
with open("dict.txt") as f:
	lines = f.readlines()
	for line in lines:
		nl = line.replace(" ", "").replace("\n", "").split(":")
		if "YES" in nl[1].upper():
			for i in range(100):
				mods[i].append(f"\t{nl[0]} = yes")
		elif "DECREASING" in nl[1].upper():
			# parse decreasing modifiers
			ostrl = nl[1].replace("DECREASING", "").split("-")
			xmin = float(ostrl[0])
			xmax = float(ostrl[1])
			for i in range(100):
				mod = xmax - (((xmax - xmin) * (100 - i - 1)) / 100.0)
				mod = round(mod, 3)
				mods[i].append(f"\t{nl[0]} = {mod}")
		elif "FIXED" in nl[1].upper():
			# parse fixed modifiers
			oint = int(nl[1].upper().replace("FIXED", ""))
			for i in range(100):
				mods[i].append(f"\t{nl[0]} = {oint}")
		else:
			# parse standard modifiers
			xmax = float(nl[1])
			for i in range(100):
				mod = round((xmax * (i + 1)) / 100.0, 3)
				mods[i].append(f"\t{nl[0]} = {mod}")
for i in range(100):
	mods[i].append("}")

with open("insane_wardec_modifiers.txt", "w") as f:
	for modifier in mods:
		for line in modifier:
			f.write(line)
			f.write("\n")
		f.write("\n\n")