from itertools import chain

arts = {
    "Shinobi": [
        "Whirlwind Slash",
        "Mikiri Counter",
        "A Shinobi's Karma: Body",
        "Mid-air Deflection",
        "Run and Slide",
        "Suppress Presence",
        "A Shinobi's Karma: Mind",
        "Breath of Life: Light",
        "Mid-air Combat Arts",
        "Shinobi Eyes",
        "Vault Over",
        "Suppress Sound",
        "Shadowrush",
    ],

    "Prosthetic": [
        "Chasing Slice",
        "Grappling Hook Attack",
        "Fang and Blade",
        "Mid-air Prosthetic Tool",
        "Nightjar Slash",
        "Emma's Medicine: Potency",
        "Projected Force",
        "Sculptor's Karma: Scars",
        "Sculptor's Karma: Blood",
        "Nightjar Slash Reversal",
        "Emma's Medicine: Aroma",
        "Living Force",
    ],
    
    "Ashina": [
        "Ichimonji",
        "Descending Carp",
        "Ascending Carp",
        "Ichimonji: Double",
        "Breath of Nature: Light",
        "Flowing Water",
        "Ashina Cross",
    ],
    
    "Temple": [
        "Praying Strikes",
        "Virtuous Deed",
        "Praying Strikes - Exorcism",
        "Most Virtuous Deed",
        "Senpou Leaping Kicks",
        "Devotion",
        "High Monk",
    ],
    
    "Mushin": [
        "Shadowfall",
        "Spiral Cloud Passage",
        "Empowered Mortal Draw",
    ],
}

skills = {
    # SHINOBI ARTS
    "Whirlwind Slash": (1, []),
    "Mikiri Counter": (2, []),
    "A Shinobi's Karma: Body": (2, ["Whirlwind Slash"]),
    "Mid-air Deflection": (1, ["Whirlwind Slash"]),
    "Run and Slide": (1, ["Mikiri Counter"]),
    "Suppress Presence": (2, ["Mikiri Counter"]),
    "A Shinobi's Karma: Mind": (3, ["A Shinobi's Karma: Body"]),
    "Breath of Life: Light": (5, ["A Shinobi's Karma: Body"]),
    "Mid-air Combat Arts": (3, ["A Shinobi's Karma: Body", "Mid-air Deflection"]),
    "Shinobi Eyes": (3, ["Run and Slide", "Suppress Presence"]),
    "Vault Over": (2, ["Suppress Presence"]),
    "Suppress Sound": (3, ["Suppress Presence"]),
    "Shadowrush": (6, ["Mid-air Combat Arts", "Shinobi Eyes"]),

    # PROSTHETIC ARTS
    "Chasing Slice": (1, []),
    "Grappling Hook Attack": (1, []),
    "Fang and Blade": (2, ["Chasing Slice"]),
    "Mid-air Prosthetic Tool": (3, ["Chasing Slice"]),
    "Nightjar Slash": (2, ["Grappling Hook Attack"]),
    "Emma's Medicine: Potency": (4, ["Grappling Hook Attack"]),
    "Projected Force": (1, ["Fang and Blade", "Mid-air Prosthetic Tool"]),
    "Sculptor's Karma: Scars": (3, ["Mid-air Prosthetic Tool"]),
    "Sculptor's Karma: Blood": (2, ["Mid-air Prosthetic Tool"]),
    "Nightjar Slash Reversal": (3, ["Nightjar Slash", "Emma's Medicine: Potency"]),
    "Emma's Medicine: Aroma": (5, ["Emma's Medicine: Potency"]),
    "Living Force": (4, ["Projected Force", "Nightjar Slash Reversal"]),
    
    # ASHINA ARTS
    "Ichimonji": (2, []),
    "Descending Carp": (1, ["Ichimonji"]),
    "Ascending Carp": (2, ["Ichimonji"]),
    "Ichimonji: Double": (3, ["Descending Carp", "Ascending Carp"]),
    "Breath of Nature: Light": (2, ["Ascending Carp"]),
    "Flowing Water": (3, ["Ascending Carp"]),
    "Ashina Cross": (5, ["Ichimonji: Double", "Flowing Water"]),
    
    # TEMPLE ARTS
    "Praying Strikes": (2, []),
    "Virtuous Deed": (3, ["Praying Strikes"]),
    "Praying Strikes - Exorcism": (3, ["Praying Strikes"]),
    "Most Virtuous Deed": (4, ["Virtuous Deed"]),
    "Senpou Leaping Kicks": (3, ["Virtuous Deed"]),
    "Devotion": (3, ["Praying Strikes - Exorcism"]),
    "High Monk": (4, ["Senpou Leaping Kicks"]),
    
    # MUSHIN ARTS
    "Shadowfall": (6, ["Shadowrush", "High Monk"]),
    "Spiral Cloud Passage": (9, ["Shadowrush", "Ashina Cross"]),
    "Empowered Mortal Draw": (5, ["Ashina Cross", "Living Force"]),
}

reqs = lambda skill_name: skills[skill_name][1]
all_reqs = lambda skill_name: list(set(reqs(skill_name) + list(chain(*map(all_reqs, reqs(skill_name))))))
base_cost = lambda skill_name: skills[skill_name][0]
skill_cost = lambda skill_name: base_cost(skill_name) + sum(map(base_cost, all_reqs(skill_name)))

total_skills_cost = lambda skills: sum(map(base_cost, list(set(list(chain(*map(all_reqs, skills))) + skills))))

print_skills_cost = lambda skills: [print(skill_name + ": " + str(skill_cost(skill_name))) for skill_name in skills]

for art_name, art_skills in arts.items():
    print("---" + art_name + "---")
    print_skills_cost(art_skills)
    print("ALL SKILLS: " + str(total_skills_cost(art_skills)))
    print()
    
print("ALL SKILLS OVERALL: " + str(sum(map(base_cost, [skill_name for skill_name, _ in skills.items()]))))
    