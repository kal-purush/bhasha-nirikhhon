import csv

products = [
    {
        "product_name": "Apple Watch 8 41 mm",
        "price": 15232,
        "discount": 10,
        "stock": 50,
        "description": "Apple Watch 8 je výkonný chytrý hodinek, které nabízí bezproblémový a intuitivní"
        " uživatelský zážitek. "
        "Svým elegantním designem a pokročilými funkcemi je perfektním společníkem pro váš každodenní život. "
        "Buďte stále připojeni, sledujte svou kondici a přistupujte ke široké škále aplikací přímo ze svého zápěstí. "
        "Apple Watch 8 disponuje živým displejem, rychlým procesorem a dostatečnou kapacitou pro uložení oblíbených "
        "aplikací a hudby. Taktéž nabízí pokročilé sledování zdraví a kondice, jako je monitorování srdečního "
        "rytmu, počítání kroků a sledování spánku. Hodinky jsou odolné vůči vodě a mají vestavěný GPS pro venkovní "
        "aktivity. Zažijte další generaci chytrých hodinek s Apple Watch 8.",
        "subheading": "Zážitek z chytrých hodinek na další úrovni.",
        "visit_count": 3762,
        "product_type": "Hodinky",
        "brand_id": 1,
        "category_id": 2,
        "color": "černá",
        "product_image": "test_image_apple_watch_8.jpeg",
        "display_size": 1.6,
        "display_resolution": "448x368",
        "operating_system": "watchOS",
        "memory": 32,
        "battery_capacity": 300,
        "weight": 30,
        "weight_units": "g",
        "bluetooth": True,
        "wifi": True,
        "nfc": True,
        "esim": True,
        "heart_rate_monitor": True,
        "step_counter": True,
        "sleep_tracker": True,
        "gps": True,
        "water_resistant": True,
        "music_player": True,
        "voice_assistant": True,
    },
    {
        "product_name": "Apple Watch 8 45 mm",
        "price": 16232,
        "discount": 10,
        "stock": 50,
        "description": "Apple Watch 8 je výkonný chytrý hodinek, které nabízí bezproblémový a intuitivní "
        "uživatelský zážitek. "
        "Svým elegantním designem a pokročilými funkcemi je perfektním společníkem pro váš každodenní život. "
        "Buďte stále připojeni, sledujte svou kondici a přistupujte ke široké škále aplikací přímo ze svého zápěstí. "
        "Apple Watch 8 disponuje živým displejem, rychlým procesorem a dostatečnou kapacitou pro uložení oblíbených "
        "aplikací a hudby. Taktéž nabízí pokročilé sledování zdraví a kondice, jako je monitorování srdečního "
        "rytmu, počítání kroků a sledování spánku. Hodinky jsou odolné vůči vodě a mají vestavěný GPS pro venkovní "
        "aktivity. Zažijte další generaci chytrých hodinek s Apple Watch 8.",
        "subheading": "Zážitek z chytrých hodinek na další úrovni.",
        "visit_count": 3762,
        "product_type": "Hodinky",
        "brand_id": 1,
        "category_id": 2,
        "color": "černá",
        "product_image": "test_image_apple_watch_8.jpeg",
        "display_size": 1.9,
        "display_resolution": "448x368",
        "operating_system": "watchOS",
        "memory": 32,
        "battery_capacity": 300,
        "weight": 30,
        "weight_units": "g",
        "bluetooth": True,
        "wifi": True,
        "nfc": True,
        "esim": True,
        "heart_rate_monitor": True,
        "step_counter": True,
        "sleep_tracker": True,
        "gps": True,
        "water_resistant": True,
        "music_player": True,
        "voice_assistant": True,
    },
    {
        "product_name": "Apple Watch 7 41 mm",
        "price": 10232,
        "discount": 0,
        "stock": 5,
        "description": "Apple Watch 7 je výkonný chytrý hodinek, které nabízí bezproblémový a "
        "intuitivní uživatelský zážitek. "
        "Svým elegantním designem a pokročilými funkcemi je perfektním společníkem pro váš každodenní život. "
        "Buďte stále připojeni, sledujte svou kondici a přistupujte ke široké škále aplikací přímo ze svého zápěstí. "
        "Apple Watch 7 disponuje živým displejem, rychlým procesorem a dostatečnou kapacitou pro uložení oblíbených "
        "aplikací a hudby. Taktéž nabízí pokročilé sledování zdraví a kondice, jako je monitorování srdečního "
        "rytmu, počítání kroků a sledování spánku. Hodinky jsou odolné vůči vodě a mají vestavěný GPS pro venkovní "
        "aktivity. Zažijte další generaci chytrých hodinek s Apple Watch 7.",
        "subheading": "Zážitek z chytrých hodinek na další úrovni.",
        "visit_count": 3762,
        "product_type": "Hodinky",
        "brand_id": 1,
        "category_id": 2,
        "color": "černá",
        "product_image": "test_image_apple_watch_8.jpeg",
        "display_size": 1.6,
        "display_resolution": "448x368",
        "operating_system": "watchOS",
        "memory": 32,
        "battery_capacity": 300,
        "weight": 30,
        "weight_units": "g",
        "bluetooth": True,
        "wifi": True,
        "nfc": True,
        "esim": True,
        "heart_rate_monitor": True,
        "step_counter": True,
        "sleep_tracker": True,
        "gps": True,
        "water_resistant": True,
        "music_player": True,
        "voice_assistant": True,
    },
    {
        "product_name": "Apple Watch 7 45 mm",
        "price": 16232,
        "discount": 10,
        "stock": 50,
        "description": "Apple Watch 7 je výkonný chytrý hodinek, které nabízí bezproblémový a"
        " intuitivní uživatelský zážitek. "
        "Svým elegantním designem a pokročilými funkcemi je perfektním společníkem pro váš každodenní život. "
        "Buďte stále připojeni, sledujte svou kondici a přistupujte ke široké škále aplikací přímo ze "
        "svého zápěstí. Apple Watch 7 disponuje živým displejem, rychlým procesorem a dostatečnou"
        " kapacitou pro uložení oblíbených aplikací a hudby. Taktéž nabízí pokročilé sledování zdraví "
        "a kondice, jako je monitorování srdečního rytmu, počítání kroků a sledování spánku. Hodinky"
        " jsou odolné vůči vodě a mají vestavěný GPS pro venkovní aktivity. Zažijte další generaci "
        "chytrých hodinek s Apple Watch 7.",
        "subheading": "Zážitek z chytrých hodinek na další úrovni.",
        "visit_count": 3762,
        "product_type": "Hodinky",
        "brand_id": 1,
        "category_id": 2,
        "color": "černá",
        "product_image": "test_image_apple_watch_8.jpeg",
        "display_size": 1.9,
        "display_resolution": "448x368",
        "operating_system": "watchOS",
        "memory": 32,
        "battery_capacity": 300,
        "weight": 30,
        "weight_units": "g",
        "bluetooth": True,
        "wifi": True,
        "nfc": True,
        "esim": True,
        "heart_rate_monitor": True,
        "step_counter": True,
        "sleep_tracker": True,
        "gps": True,
        "water_resistant": True,
        "music_player": True,
        "voice_assistant": True,
    },
    {
        "product_name": "Apple Watch Ultra 49 mm",
        "price": 19999,
        "discount": 10,
        "stock": 20,
        "description": "Apple Watch Ultra je prvotřídní chytré hodinky, které nabízejí plynulý a intuitivní"
        " uživatelský zážitek. Díky svému elegantnímu designu a pokročilým funkcím jsou "
        "ideálním společníkem pro váš každodenní život. Zůstaňte neustále propojeni, "
        "sledujte svou kondici a získávejte přístup k široké škále aplikací přímo na svém "
        "zápěstí. Apple Watch Ultra disponuje velkým živým displejem, výkonným procesorem "
        "a dostatečnou kapacitou pro uložení oblíbených aplikací a hudby. Nabízí také pokročilé"
        " sledování zdraví a kondice, včetně monitorování srdečního tepu, počítání kroků a sledování"
        " spánku. Hodinky jsou odolné vůči vodě a mají vestavěný GPS pro venkovní aktivity. "
        "Zažijte novou úroveň chytrých hodinek s Apple Watch Ultra.",
        "subheading": "Vylepšete svůj zážitek s chytrými hodinkami.",
        "visit_count": 4628,
        "product_type": "Hodinky",
        "brand_id": 1,
        "category_id": 2,
        "color": "černá",
        "product_image": "test_image_apple_watch_8.jpeg",
        "display_size": 1.9,
        "display_resolution": "512x384",
        "operating_system": "watchOS",
        "memory": 32,
        "battery_capacity": 350,
        "weight": 50,
        "weight_units": "g",
        "bluetooth": True,
        "wifi": True,
        "nfc": True,
        "esim": True,
        "heart_rate_monitor": True,
        "step_counter": True,
        "sleep_tracker": True,
        "gps": True,
        "water_resistant": True,
        "music_player": True,
        "voice_assistant": True,
    },
    {
        "product_name": "Apple Watch SE 40 mm",
        "price": 10999,
        "discount": 5,
        "stock": 100,
        "description": "Apple Watch SE je cenově dostupná verze chytrých hodinek, které nabízí vynikající"
        " uživatelský zážitek. S elegantním designem a pokročilými funkcemi je ideálním"
        " společníkem pro váš každodenní život. Zůstaňte připojeni, sledujte svou kondici "
        "a získávejte přístup k množství aplikací přímo ze svého zápěstí. Apple Watch SE "
        "disponuje jasným displejem, výkonným procesorem a dostatečnou kapacitou pro uložení "
        "vašich oblíbených aplikací a hudby. Sledujte své zdraví a kondici díky monitorování "
        "srdečního tepu, počítání kroků a sledování spánku. Hodinky jsou odolné vůči vodě a mají"
        " vestavěný GPS pro venkovní aktivity. Vylepšete svůj životní styl s Apple Watch SE.",
        "subheading": "Cenově dostupné a plnohodnotné chytré hodinky.",
        "visit_count": 7895,
        "product_type": "Hodinky",
        "brand_id": 1,
        "category_id": 2,
        "color": "stříbrná",
        "product_image": "test_image_apple_watch_8.jpeg",
        "display_size": 1.57,
        "display_resolution": "394x324",
        "operating_system": "watchOS",
        "memory": 32,
        "battery_capacity": 285,
        "weight": 36,
        "weight_units": "g",
        "bluetooth": True,
        "wifi": True,
        "nfc": True,
        "esim": True,
        "heart_rate_monitor": True,
        "step_counter": True,
        "sleep_tracker": True,
        "gps": True,
        "water_resistant": True,
        "music_player": True,
        "voice_assistant": True,
    },
    {
        "product_name": "Apple Watch SE 44 mm",
        "price": 11999,
        "discount": 7,
        "stock": 80,
        "description": "Apple Watch SE je cenově dostupná verze chytrých hodinek, které nabízí vynikající "
        "uživatelský zážitek. S elegantním designem a pokročilými funkcemi je ideálním "
        "společníkem pro váš každodenní život. Zůstaňte připojeni, sledujte svou kondici "
        "a získávejte přístup k množství aplikací přímo ze svého zápěstí. Apple Watch SE "
        "disponuje jasným displejem, výkonným procesorem a dostatečnou kapacitou pro uložení "
        "vašich oblíbených aplikací a hudby. Sledujte své zdraví a kondici díky monitorování "
        "srdečního tepu, počítání kroků a sledování spánku. Hodinky jsou odolné vůči vodě a mají "
        "vestavěný GPS pro venkovní aktivity. Vylepšete svůj životní styl s Apple Watch SE.",
        "subheading": "Cenově dostupné a plnohodnotné chytré hodinky.",
        "visit_count": 5832,
        "product_type": "Hodinky",
        "brand_id": 1,
        "category_id": 2,
        "color": "černá",
        "product_image": "test_image_apple_watch_8.jpeg",
        "display_size": 1.78,
        "display_resolution": "448x368",
        "operating_system": "watchOS",
        "memory": 32,
        "battery_capacity": 285,
        "weight": 47,
        "weight_units": "g",
        "bluetooth": True,
        "wifi": True,
        "nfc": True,
        "esim": True,
        "heart_rate_monitor": True,
        "step_counter": True,
        "sleep_tracker": True,
        "gps": True,
        "water_resistant": True,
        "music_player": True,
        "voice_assistant": True,
    },
    {
        "product_name": "Apple Watch Series 6 44 mm",
        "price": 15999,
        "discount": 15,
        "stock": 60,
        "description": "Apple Watch Series 6 je nejnovější model chytrých hodinek s pokročilými funkcemi"
        " a špičkovým výkonem. S jeho výrazným designem a širokou škálou funkcí je perfektním "
        "společníkem pro váš každodenní život. Zůstaňte připojeni, monitorujte své zdraví a kondici, "
        "a získávejte přístup k mnoha užitečným aplikacím přímo ze svého zápěstí. Apple Watch Series 6"
        " disponuje jasným a barevným displejem, výkonným procesorem a dostatečnou kapacitou pro"
        " uložení vašich oblíbených aplikací a hudby. Sledujte své srdeční tepové frekvence, měřte"
        " hladinu kyslíku v krvi, monitorujte spánek, a využívejte vestavěný GPS pro přesné sledování "
        "aktivit. Hodinky jsou voděodolné a nabízejí vestavěnou hlasovou asistentku."
        " Vylepšete svůj životní styl s Apple Watch Series 6.",
        "subheading": "Pokročilé chytré hodinky pro každodenní život.",
        "visit_count": 7645,
        "product_type": "Hodinky",
        "brand_id": 1,
        "category_id": 2,
        "color": "stříbrná",
        "product_image": "test_image_apple_watch_8.jpeg",
        "display_size": 1.78,
        "display_resolution": "448x368",
        "operating_system": "watchOS",
        "memory": 32,
        "battery_capacity": 303.8,
        "weight": 47.1,
        "weight_units": "g",
        "bluetooth": True,
        "wifi": True,
        "nfc": True,
        "esim": True,
        "heart_rate_monitor": True,
        "step_counter": True,
        "sleep_tracker": True,
        "gps": True,
        "water_resistant": True,
        "music_player": True,
        "voice_assistant": True,
    },
    {
        "product_name": "Samsung Galaxy Watch 5 44mm",
        "price": 10999,
        "discount": 12,
        "stock": 40,
        "description": "Samsung Galaxy Watch 5 je pokročilé chytré hodinky s moderním designem a"
                       " špičkovými funkcemi. S jeho inovativními technologiemi a elegantním vzhledem je "
                       "perfektním společníkem pro váš každodenní život. Zůstaňte připojeni, sledujte svou "
                       "kondici a získávejte přístup k různým funkcím přímo ze svého zápěstí. Samsung Galaxy "
                       "Watch 5 disponuje jasným a barevným displejem, výkonným procesorem a dostatečnou "
                       "kapacitou pro uložení vašich oblíbených aplikací a hudby. Měřte srdeční tep,"
                       " monitorujte spánek, sledujte kroky a získejte přesné údaje o svých aktivitách "
                       "pomocí vestavěného GPS. Hodinky jsou odolné vůči vodě a nabízejí rozsáhlé možnosti "
                       "připojení. Užijte si špičkové chytré hodinky s Samsung Galaxy Watch 5.",
        "subheading": "Pokročilé chytré hodinky pro každodenní život.",
        "visit_count": 5241,
        "product_type": "Hodinky",
        "brand_id": 2,
        "category_id": 2,
        "color": "černá",
        "product_image": "test_image_samsung_galaxy_watch.jpeg",
        "display_size": 1.4,
        "display_resolution": "360x360",
        "operating_system": "Tizen",
        "memory": 8,
        "battery_capacity": 340,
        "weight": 42,
        "weight_units": "g",
        "bluetooth": True,
        "wifi": True,
        "nfc": True,
        "esim": True,
        "heart_rate_monitor": True,
        "step_counter": True,
        "sleep_tracker": True,
        "gps": True,
        "water_resistant": True,
        "music_player": True,
        "voice_assistant": True,
    },
    {
        "product_name": "Samsung Galaxy Watch 5 40mm",
        "price": 9999,
        "discount": 10,
        "stock": 30,
        "description": "Samsung Galaxy Watch 5 je pokročilé chytré hodinky s moderním designem a špičkovými funkcemi."
                       " S jeho inovativními technologiemi a elegantním vzhledem je perfektním společníkem pro váš "
                       "každodenní život. Zůstaňte připojeni, sledujte svou kondici a získávejte přístup k různým "
                       "funkcím přímo ze svého zápěstí. Samsung Galaxy Watch 5 disponuje jasným a barevným displejem,"
                       " výkonným procesorem a dostatečnou kapacitou pro uložení vašich oblíbených aplikací a hudby."
                       " Měřte srdeční tep, monitorujte spánek, sledujte kroky a získejte přesné údaje o svých "
                       "aktivitách pomocí vestavěného GPS. Hodinky jsou odolné vůči vodě a nabízejí rozsáhlé možnosti "
                       "připojení. Užijte si špičkové chytré hodinky s Samsung Galaxy Watch 5.",
        "subheading": "Pokročilé chytré hodinky pro každodenní život.",
        "visit_count": 4321,
        "product_type": "Hodinky",
        "brand_id": 2,
        "category_id": 2,
        "color": "černá",
        "product_image": "test_image_samsung_galaxy_watch.jpeg",
        "display_size": 1.2,
        "display_resolution": "360x360",
        "operating_system": "Tizen",
        "memory": 4,
        "battery_capacity": 247,
        "weight": 37,
        "weight_units": "g",
        "bluetooth": True,
        "wifi": True,
        "nfc": True,
        "esim": True,
        "heart_rate_monitor": True,
        "step_counter": True,
        "sleep_tracker": True,
        "gps": True,
        "water_resistant": True,
        "music_player": True,
        "voice_assistant": True,
    },
    {
        "product_name": "Samsung Galaxy Watch 4 40 mm",
        "price": 8499,
        "discount": 15,
        "stock": 25,
        "description": "Samsung Galaxy Watch 4 je špičkové chytré hodinky s elegantním designem a pokročilými funkcemi."
        "Sledujte svou kondici, monitorujte srdeční tep, počítání kroků a spánek, a využijte širokou škálu "
        "aplikací přímo ze svého zápěstí. Disponuje živým displejem s vysokým rozlišením, který poskytuje "
        "jasné a ostré zobrazení. Hodinky jsou vybaveny vestavěným GPS, senzorem srdečního tepu, voděodolností "
        "a dlouhou výdrží baterie. S Galaxy Watch 4 budete vždy připojeni a můžete se spolehnout na jejich "
        "přesné měření a spolehlivost.",
        "subheading": "Elegantní a výkonné chytré hodinky pro každodenní život.",
        "visit_count": 2389,
        "product_type": "Hodinky",
        "brand_id": 2,
        "category_id": 2,
        "color": "Černá",
        "product_image": "test_image_samsung_galaxy_watch.jpeg",
        "display_size": 1.4,
        "display_resolution": "450x450",
        "operating_system": "Tizen",
        "memory": 8,
        "battery_capacity": 247,
        "weight": 25,
        "weight_units": "g",
        "bluetooth": True,
        "wifi": True,
        "nfc": True,
        "esim": False,
        "heart_rate_monitor": True,
        "step_counter": True,
        "sleep_tracker": True,
        "gps": True,
        "water_resistant": True,
        "music_player": True,
        "voice_assistant": True,
    },
    {
        "product_name": "Samsung Galaxy Watch 4 46 mm",
        "price": 9499,
        "discount": 12,
        "stock": 30,
        "description": "The Samsung Galaxy Watch 4 is a powerful smartwatch with a sleek design and advanced features. "
        "Monitor your fitness, track your heart rate, count steps, and analyze your sleep patterns with ease. "
        "With its vibrant display, the watch provides clear and vivid visuals. It comes with built-in GPS, "
        "heart rate sensor, water resistance, and long-lasting battery life. Stay connected and rely on its "
        "accurate measurements and reliability throughout the day.",
        "subheading": "A sophisticated and feature-rich smartwatch for your daily life.",
        "visit_count": 1786,
        "product_type": "Hodinky",
        "brand_id": 2,
        "category_id": 2,
        "color": "Černá",
        "product_image": "test_image_samsung_galaxy_watch.jpeg",
        "display_size": 1.8,
        "display_resolution": "450x450",
        "operating_system": "Tizen",
        "memory": 16,
        "battery_capacity": 361,
        "weight": 48,
        "weight_units": "g",
        "bluetooth": True,
        "wifi": True,
        "nfc": True,
        "esim": False,
        "heart_rate_monitor": True,
        "step_counter": True,
        "sleep_tracker": True,
        "gps": True,
        "water_resistant": True,
        "music_player": True,
        "voice_assistant": True,
    },
]

with open("watch_products.csv", "w", newline="") as file:
    # create a writer object
    writer = csv.writer(file)

    # write the header row
    writer.writerow(products[0].keys())

    # write the data rows
    for product in products:
        writer.writerow(product.values())