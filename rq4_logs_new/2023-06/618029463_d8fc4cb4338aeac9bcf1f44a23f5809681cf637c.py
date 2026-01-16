import csv

products = [
    {
        "product_name": "Philips 32M1N5500VS Gaming",
        "price": 5960,
        "discount": 3,
        "stock": 3,
        "sold": 0,
        "description": "Philips 32M1N5500VS Gaming je herní monitor s vysokým výkonem "
        "a špičkovými funkcemi. S úhlopříčkou 32 palců a rozlišením 2560 x 1440 "
        "nabízí ostrý a detailní obraz. S frekvencí obnovování 144 Hz a odezvou 1 ms "
        "zajišťuje plynulé a rychlé reakce ve hrách. Displej s poměrem stran 16:9 a "
        "barevnou hloubkou 8 bitů přináší věrné a syté barevné podání. Monitor disponuje "
        "různými konektory, včetně HDMI, DisplayPort a USB, pro snadné připojení periferních "
        "zařízení. Navíc má vestavěné reproduktory, nastavitelný stojan a možnost montáže na stěnu. "
        "Philips 32M1N5500VS Gaming je energeticky úsporný monitor s hodnocením A.",
        "subheading": "Herní monitor Philips 32M1N5500VS Gaming",
        "visit_count": 1520,
        "product_type": "Monitory",
        "brand_id": 11,
        "category_id": 5,
        "product_image": "test_monitor_image.jpeg",
        "display_size": 32,
        "display_resolution": "2560 x 1440",
        "refresh_rate": 144,
        "response_time": 1,
        "aspect_ratio": "16:9",
        "connectivity": "HDMI, DisplayPort, USB",
        "color_depth": 8,
        "height": 45.4,
        "height_units": "cm",
        "width": 73.4,
        "width_units": "cm",
        "depth": 7.6,
        "depth_units": "cm",
        "weight": 6.2,
        "weight_units": "kg",
        "color": "černá",
        "curvature": False,
        "adjustable_stand": True,
        "wall_mountable": True,
        "built_in_speakers": True,
        "energy_efficiency": "A",
    },
    {
        "product_name": "Samsung C34H890",
        "price": 14990,
        "discount": 3,
        "stock": 3,
        "sold": 0,
        "description": "Samsung C34H890 je špičkový monitor s vysokou kvalitou obrazu a pohodlným "
        "uživatelským prostředím. S úhlopříčkou 34 palců a rozlišením 3440 x 1440 "
        "nabízí širokou obrazovou plochu s vysokým detailním rozlišením. S frekvencí "
        "obnovování 60 Hz a odezvou 4 ms je ideální pro běžné použití a práci s multimédii. "
        "Displej s poměrem stran 21:9 a barevnou hloubkou 8 bitů přináší přirozené a živé "
        "barevné podání. Monitor disponuje různými konektory, včetně HDMI, DisplayPort a USB, "
        "pro snadné připojení periferních zařízení. Navíc má nastavitelný stojan a možnost "
        "montáže na stěnu. Samsung C34H890 je energeticky úsporný monitor s hodnocením A.",
        "subheading": "Samsung C34H890 - Špičkový monitor",
        "visit_count": 1520,
        "product_type": "Monitory",
        "brand_id": 2,
        "category_id": 5,
        "product_image": "test_monitor_image.jpeg",
        "display_size": 34,
        "display_resolution": "3440 x 1440",
        "refresh_rate": 60,
        "response_time": 4,
        "aspect_ratio": "21:9",
        "connectivity": "HDMI, DisplayPort, USB",
        "color_depth": 8,
        "height": 45.4,
        "height_units": "cm",
        "width": 73.4,
        "width_units": "cm",
        "depth": 7.6,
        "depth_units": "cm",
        "weight": 6.2,
        "weight_units": "kg",
        "color": "černá",
        "curvature": False,
        "adjustable_stand": True,
        "wall_mountable": True,
        "built_in_speakers": True,
        "energy_efficiency": "A",
    },
    {
        "product_name": "MSI Oculux NXG253R",
        "price": 7499,
        "discount": 10,
        "stock": 5,
        "sold": 0,
        "description": "MSI Oculux NXG253R je výkonný herní monitor s vysokou obnovovací frekvencí "
        "a nízkou odezvou, který nabízí plynulé a responsivní zobrazení. S úhlopříčkou "
        "24.5 palce a rozlišením 1920 x 1080 poskytuje ostrý a detailní obraz. S frekvencí "
        "obnovování 240 Hz a odezvou 1 ms je ideální pro akční hry a e-sportovní soutěže. "
        "Monitor podporuje technologie Adaptive Sync a G-Sync, které minimalizují trhání obrazu "
        "a zajišťují hladkou herní grafiku. Displej s poměrem stran 16:9 a barevnou hloubkou 8 bitů "
        "přináší věrné a syté barvy. Monitor je vybaven různými konektory, včetně HDMI, DisplayPort "
        "a USB, pro snadné připojení periferních zařízení. MSI Oculux NXG253R je vysoce výkonný monitor "
        "pro váš herní setup.",
        "subheading": "MSI Oculux NXG253R - Herní monitor",
        "visit_count": 3100,
        "product_type": "Monitory",
        "brand_id": 8,
        "category_id": 5,
        "product_image": "test_monitor_image.jpeg",
        "display_size": 24.5,
        "display_resolution": "1920 x 1080",
        "refresh_rate": 240,
        "response_time": 1,
        "aspect_ratio": "16:9",
        "connectivity": "HDMI, DisplayPort, USB",
        "color_depth": 8,
        "height": 36.6,
        "height_units": "cm",
        "width": 55.9,
        "width_units": "cm",
        "depth": 24.8,
        "depth_units": "cm",
        "weight": 4.2,
        "weight_units": "kg",
        "color": "černá",
        "curvature": False,
        "adjustable_stand": True,
        "wall_mountable": True,
        "built_in_speakers": False,
        "energy_efficiency": "A+",
    },
    {
        "product_name": "Philips 279M1RV",
        "price": 8999,
        "discount": 5,
        "stock": 10,
        "sold": 0,
        "description": "Philips 279M1RV je kvalitní monitor s vysokým rozlišením a širokým úhlem "
        "pohledu, který nabízí skvělé vizuální zážitky. S úhlopříčkou 27 palců a rozlišením "
        "2560 x 1440 poskytuje ostrý a detailní obraz. Díky technologii IPS má monitor široké "
        "úhly pohledu a věrné barevné podání. S frekvencí obnovování 75 Hz a odezvou 4 ms je "
        "vhodný pro různé typy her a multimediální obsah. Displej s poměrem stran 16:9 a barevnou "
        "hloubkou 8 bitů přináší realistické a syté barvy. Monitor disponuje různými konektory, "
        "včetně HDMI, DisplayPort a USB, pro snadné připojení periferních zařízení. Díky svému "
        "elegantnímu designu a výkonným funkcím je Philips 279M1RV skvělou volbou pro domácí "
        "zábavu i profesionální využití.",
        "subheading": "Philips 279M1RV - Kvalitní monitor",
        "visit_count": 5000,
        "product_type": "Monitory",
        "brand_id": 11,
        "category_id": 5,
        "product_image": "test_monitor_image.jpeg",
        "display_size": 27,
        "display_resolution": "2560 x 1440",
        "refresh_rate": 75,
        "response_time": 4,
        "aspect_ratio": "16:9",
        "connectivity": "HDMI, DisplayPort, USB",
        "color_depth": 8,
        "height": 46.4,
        "height_units": "cm",
        "width": 61.3,
        "width_units": "cm",
        "depth": 23.4,
        "depth_units": "cm",
        "weight": 5.8,
        "weight_units": "kg",
        "color": "černá",
        "curvature": False,
        "adjustable_stand": True,
        "wall_mountable": True,
        "built_in_speakers": True,
        "energy_efficiency": "A",
    },
    {
        "product_name": "Samsung Odyssey G85SB",
        "price": 12999,
        "discount": 10,
        "stock": 5,
        "sold": 0,
        "description": "Samsung Odyssey G85SB je výkonný herní monitor s zakřiveným displejem, který "
        "poskytuje ohromující vizuální zážitek. S úhlopříčkou 34 palců a rozlišením "
        "3440 x 1440 nabízí široký obrazový prostor pro pohlcující hraní her a "
        "multimediální obsah. Díky technologii QLED a barevnému gamutu sRGB 125% "
        "zajišťuje realistické a syté barvy. S frekvencí obnovování 144 Hz a "
        "odezvou 1 ms je ideální pro rychlé a akční scény ve hrách. Monitor je "
        "vybaven různými konektory, včetně HDMI, DisplayPort a USB, pro snadné "
        "připojení periferních zařízení. S elegantním designem a výkonnými funkcemi "
        "je Samsung Odyssey G85SB skvělou volbou pro váš herní setup.",
        "subheading": "Samsung Odyssey G85SB - Výkonný herní monitor",
        "visit_count": 2500,
        "product_type": "Monitory",
        "brand_id": 2,
        "category_id": 5,
        "product_image": "test_monitor_image_1.jpeg",
        "display_size": 34,
        "display_resolution": "3440 x 1440",
        "refresh_rate": 144,
        "response_time": 1,
        "aspect_ratio": "21:9",
        "connectivity": "HDMI, DisplayPort, USB",
        "color_depth": 8,
        "height": 43.1,
        "height_units": "cm",
        "width": 80.9,
        "width_units": "cm",
        "depth": 31.6,
        "depth_units": "cm",
        "weight": 7.2,
        "weight_units": "kg",
        "color": "černá",
        "curvature": True,
        "adjustable_stand": True,
        "wall_mountable": True,
        "built_in_speakers": False,
        "energy_efficiency": "B",
    },
    {
        "product_name": "Dell S2721DS",
        "price": 3799,
        "discount": 5,
        "stock": 8,
        "sold": 0,
        "description": "Dell S2721DS je špičkový monitor s vysokým rozlišením, který poskytuje "
        "kvalitní obrazový zážitek. S úhlopříčkou 27 palců a rozlišením 2560 x 1440 "
        "nabízí ostrý a detailní obraz. Díky technologii IPS a širokému barevnému "
        "gamutu sRGB 99% zobrazuje přesné a živé barvy. S frekvencí obnovování 75 Hz "
        "a odezvou 4 ms je vhodný pro běžné použití i hraní her. Monitor je vybaven "
        "různými konektory, včetně HDMI a DisplayPort, pro snadné připojení periferních "
        "zařízení. S tenkým rámečkem a elegantním designem se skvěle hodí do moderního "
        "pracovního prostředí. Dell S2721DS je spolehlivým a výkonným monitorem pro vaše "
        "každodenní potřeby.",
        "subheading": "Dell S2721DS - Kvalitní monitor s vysokým rozlišením",
        "visit_count": 3400,
        "product_type": "Monitory",
        "brand_id": 5,
        "category_id": 5,
        "product_image": "test_monitor_image_1.jpeg",
        "display_size": 27,
        "display_resolution": "2560 x 1440",
        "refresh_rate": 75,
        "response_time": 4,
        "aspect_ratio": "16:9",
        "connectivity": "HDMI, DisplayPort",
        "color_depth": 8,
        "height": 46.7,
        "height_units": "cm",
        "width": 61.1,
        "width_units": "cm",
        "depth": 18.3,
        "depth_units": "cm",
        "weight": 4.63,
        "weight_units": "kg",
        "color": "černá",
        "curvature": False,
        "adjustable_stand": True,
        "wall_mountable": True,
        "built_in_speakers": False,
        "energy_efficiency": "A",
    },
    {
        "product_name": "MSI MPG ARTYMIS 343CQR",
        "price": 8999,
        "discount": 10,
        "stock": 5,
        "sold": 0,
        "description": "MSI MPG ARTYMIS 343CQR je špičkový herní monitor s ultrapanoramatickým "
        "zobrazením. S úhlopříčkou 34 palců a rozlišením 3440 x 1440 nabízí "
        "ohromující obrazový zážitek. Díky zakřivenému VA panelu s technologií "
        "Quantum Dot zobrazuje živé a syté barvy. S frekvencí obnovování 165 Hz "
        "a odezvou 1 ms je ideální pro akční hry a plynulé animace. Monitor je "
        "vybaven funkcemi jako Adaptive Sync a HDR pro ještě lepší vizuální "
        "kvalitu. S rozhraními HDMI, DisplayPort a USB-C umožňuje snadné "
        "připojení různých zařízení. MSI MPG ARTYMIS 343CQR je navržený tak, "
        "aby vám poskytl nejlepší herní zážitek s vysokou kvalitou obrazu a "
        "plynulými animacemi.",
        "subheading": "MSI MPG ARTYMIS 343CQR - Ultrapanoramatický herní monitor",
        "visit_count": 4200,
        "product_type": "Monitory",
        "brand_id": 8,
        "category_id": 5,
        "product_image": "test_monitor_image_1.jpeg",
        "display_size": 34,
        "display_resolution": "3440 x 1440",
        "refresh_rate": 165,
        "response_time": 1,
        "aspect_ratio": "21:9",
        "connectivity": "HDMI, DisplayPort, USB-C",
        "color_depth": 10,
        "height": 51.4,
        "height_units": "cm",
        "width": 81.4,
        "width_units": "cm",
        "depth": 24.2,
        "depth_units": "cm",
        "weight": 9.5,
        "weight_units": "kg",
        "color": "černá",
        "curvature": True,
        "adjustable_stand": True,
        "wall_mountable": True,
        "built_in_speakers": True,
        "energy_efficiency": "A+",
    },
    {
        "product_name": "Samsung Odyssey C49RG90",
        "price": 12999,
        "discount": 15,
        "stock": 2,
        "sold": 0,
        "description": "Samsung Odyssey C49RG90 je úchvatný 49palcový ultrawide herní monitor,"
        " který nabízí pohlcující vizuální zážitek. S rozlišením 5120 x 1440 a "
        "poměrem stran 32:9 poskytuje rozsáhlý a detailní displej. Monitor "
        "disponuje zakřiveným VA panelem s technologií Quantum Dot, která "
        "dodává živé a realistické barvy. S obnovovací frekvencí 120 Hz a "
        "odezvou 4 ms zajišťuje plynulou a rychlou odezvu ve hrách. Monitor"
        " podporuje obsah HDR pro vylepšený kontrast a jas. Nabízí také různé "
        "možnosti připojení, včetně HDMI, DisplayPort a USB, které umožňují "
        "připojení různých zařízení. Samsung Odyssey C49RG90 je navržen tak, "
        "aby poskytoval bezkonkurenční herní zážitek díky svému pohlcujícímu "
        "displeji a pokročilým funkcím.",
        "subheading": "Samsung Odyssey C49RG90 - Ultrawide herní monitor",
        "visit_count": 2800,
        "product_type": "Monitory",
        "brand_id": 2,
        "category_id": 5,
        "product_image": "test_monitor_image_1.jpeg",
        "display_size": 49,
        "display_resolution": "5120 x 1440",
        "refresh_rate": 120,
        "response_time": 4,
        "aspect_ratio": "32:9",
        "connectivity": "HDMI, DisplayPort, USB",
        "color_depth": 10,
        "height": 36.5,
        "height_units": "cm",
        "width": 119.3,
        "width_units": "cm",
        "depth": 38.9,
        "depth_units": "cm",
        "weight": 14.2,
        "weight_units": "kg",
        "color": "černá",
        "curvature": True,
        "adjustable_stand": True,
        "wall_mountable": True,
        "built_in_speakers": False,
        "energy_efficiency": "A",
    },
]

with open("monitor_products.csv", "w", newline="") as file:
    # create a writer object
    writer = csv.writer(file)

    # write the header row
    writer.writerow(products[0].keys())

    # write the data rows
    for product in products:
        writer.writerow(product.values())