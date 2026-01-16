import random
import tkinter as tk
from tkinter import ttk, messagebox

skeletons_info = {}

def get_skeleton_info():
    global skeletons_info
    skeletons_info.clear()
    try:
        with open("skeleton_info.txt", "r") as file:
            for line in file:
                parts = line.strip().split(", ")
                skeleton_name = parts[0].split(": ")[1]
                bonus_to_hit = parts[1].split(": ")[1]
                bonus_to_damage = parts[2].split(": ")[1]
                damage_die = parts[3].split(": ")[1]
                crit_range = parts[4].split(": ")[1]
                skeletons_info[skeleton_name] = [bonus_to_hit, bonus_to_damage, damage_die, crit_range]
        update_dropdown()
    except FileNotFoundError:
        pass

def update_dropdown():
    type_dropdown['values'] = list(skeletons_info.keys())

def parse_dice_string(dice_string):
    parts = dice_string.split('d')
    num_dice = int(parts[0])
    dice_sides = int(parts[1])
    return num_dice, dice_sides

def parse_crit_range(crit_string):
    crit_range = []
    temp_crit = crit_string.split('-')
    temp_crit2 = []
    if len(temp_crit) > 1:
        temp_crit2 = [int(temp_crit[0]), int(temp_crit[1])]
    else:
        temp_crit2 = [int(temp_crit[0])]
    crit_range.append(temp_crit2[0])
    if len(temp_crit2) > 1:
        temp_num = crit_range[0]
        while temp_num < temp_crit2[1]:
            temp_num += 1
            crit_range.append(temp_num)
    return crit_range


def roll_action():
    results_text.insert(tk.END, "---------------------------------\n")
    skeleton_stats = skeletons_info[type_var.get()]
    num_skeletons = int(quantity_entry.get())
    attack_mods = int(attack_mods_entry.get())
    damage_mods = int(damage_mods_entry.get())
    enemy_ac = int(enemy_ac_entry.get())
    
    num_dice, dice_sides = parse_dice_string(skeleton_stats[2])

    #get the crit ranges, very janky
    crit_range = parse_crit_range(skeleton_stats[3])

    #calculate the results
    result_string = ""
    num_hits = 0
    num_crits = 0
    total_damage = 0
    for i in range(num_skeletons):
        to_hit = random.randint(1, 20) #+ int(skeleton_stats['bonus_to_hit']) + attack_mods
        if to_hit in crit_range:
            confirm_crit = random.randint(1, 20) + int(skeleton_stats[1]) + attack_mods
            if confirm_crit >= enemy_ac:
                num_crits += 1
                rolls = [random.randint(1, dice_sides) for _ in range(num_dice)]
                damage = 2 * (sum(rolls) + int(skeleton_stats[1]) + damage_mods)
                result_string += f"\nCrit with {to_hit}, dealing {damage} damage"
                total_damage += damage
            else:
                num_hits += 1
                rolls = [random.randint(1, dice_sides) for _ in range(num_dice)]
                damage = sum(rolls) + int(skeleton_stats[1]) + damage_mods
                result_string += f"\nHit with {to_hit}, dealing {damage} damage"
                total_damage += damage
        else:
            to_hit += int(skeleton_stats[1]) + attack_mods
            if to_hit < enemy_ac:
                continue
            else:
                num_hits += 1
                rolls = [random.randint(1, dice_sides) for _ in range(num_dice)]
                damage = sum(rolls) + int(skeleton_stats[1]) + damage_mods
                result_string += f"\nHit with {to_hit}, dealing {damage} damage"
                total_damage += damage
                
    result_string += f"\nTOTALS: {num_hits} HITS, {num_crits} CRITS, {total_damage} DAMAGE\n"
    results_text.insert(tk.END, result_string)

def submit_skeleton_info():
    skeleton_name = skeleton_name_entry.get()
    bonus_to_hit = bonus_to_hit_entry.get()
    bonus_to_damage = bonus_to_damage_entry.get()
    damage_die = damage_die_entry.get()
    crit_range = crit_range_entry.get()
    with open("skeleton_info.txt", "a") as file:
        file.write(f"Skeleton name: {skeleton_name}, Bonus to hit: {bonus_to_hit}, Bonus to damage: {bonus_to_damage}, Damage die: {damage_die}, Crit range: {crit_range}\n")
    messagebox.showinfo("Submitted", "Skeleton info saved successfully")
    get_skeleton_info()

# Initialize the main window
root = tk.Tk()
root.title("Skeleton Attack Simulator")

# Create and place widgets on the main window
# Type dropdown
type_label = tk.Label(root, text="Type")
type_label.place(x=50, y=50)
type_var = tk.StringVar()
type_dropdown = ttk.Combobox(root, textvariable=type_var)
type_dropdown.place(x=50, y=70, width=150)

# Quantity entry
quantity_label = tk.Label(root, text="Quantity")
quantity_label.place(x=200, y=50)
quantity_entry = tk.Entry(root)
quantity_entry.place(x=200, y=70, width=50)
quantity_entry.insert(0, "10")

# Attack Mods entry
attack_mods_label = tk.Label(root, text="Attack Mods")
attack_mods_label.place(x=300, y=50)
attack_mods_entry = tk.Entry(root)
attack_mods_entry.place(x=300, y=70, width=50)
attack_mods_entry.insert(0, "0")

# Damage Mods entry
damage_mods_label = tk.Label(root, text="Damage Mods")
damage_mods_label.place(x=400, y=50)
damage_mods_entry = tk.Entry(root)
damage_mods_entry.place(x=400, y=70, width=50)
damage_mods_entry.insert(0, "0")

# Enemy AC entry
enemy_ac_label = tk.Label(root, text="Enemy AC")
enemy_ac_label.place(x=500, y=50)
enemy_ac_entry = tk.Entry(root)
enemy_ac_entry.place(x=500, y=70, width=50)
enemy_ac_entry.insert(0, "0")

# MORE skeletons button
more_skeletons_button = tk.Button(root, text="MORE skeletons")
more_skeletons_button.place(x=200, y=120, width=100)

# Roll button
roll_button = tk.Button(root, text="Roll!", command=roll_action)
roll_button.place(x=650, y=50, width=100, height=100)

# Results label
results_label = tk.Label(root, text="Results")
results_label.place(x=600, y=200)

# Results text box
results_text = tk.Text(root, height=10, width=40)
results_text.place(x=600, y=230)

# New skeleton section
new_skeleton_label = tk.Label(root, text="New Skeleton")
new_skeleton_label.place(x=50, y=300)

# Skeleton name entry
skeleton_name_label = tk.Label(root, text="Skeleton name")
skeleton_name_label.place(x=50, y=330)
skeleton_name_entry = tk.Entry(root)
skeleton_name_entry.place(x=150, y=330, width=150)

# Bonus to hit entry
bonus_to_hit_label = tk.Label(root, text="Bonus to hit")
bonus_to_hit_label.place(x=50, y=360)
bonus_to_hit_entry = tk.Entry(root)
bonus_to_hit_entry.place(x=150, y=360, width=50)

# Bonus to damage entry
bonus_to_damage_label = tk.Label(root, text="Bonus to damage")
bonus_to_damage_label.place(x=50, y=390)
bonus_to_damage_entry = tk.Entry(root)
bonus_to_damage_entry.place(x=150, y=390, width=50)

# Damage die entry
damage_die_label = tk.Label(root, text="Damage die")
damage_die_label.place(x=50, y=420)
damage_die_entry = tk.Entry(root)
damage_die_entry.place(x=150, y=420, width=50)

# Crit range entry
crit_range_label = tk.Label(root, text="Crit range")
crit_range_label.place(x=50, y=450)
crit_range_entry = tk.Entry(root)
crit_range_entry.place(x=150, y=450, width=50)

# Submit button for new skeleton
submit_skeleton_button = tk.Button(root, text="Submit", command=submit_skeleton_info)
submit_skeleton_button.place(x=150, y=480)

# Load existing skeleton info when the program initializes
get_skeleton_info()

# Update idle tasks to calculate the required window size
root.update_idletasks()

# Set the minimum size and geometry to fit all elements
root.minsize(root.winfo_reqwidth(), root.winfo_reqheight())
root.geometry(f"{root.winfo_reqwidth()}x{root.winfo_reqheight()}")

# Start the main event loop
root.mainloop()