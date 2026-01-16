import os


PSQL_USER = os.environ['PSQL_USER']
PSQL_PASSWORD = os.environ['PSQL_PASSWORD']
HOST = 'localhost:5432'


GOODS = ['Skirt', 'Bikini', 'Dress Pants', 'Jumper', 'Sneakers', 'Hoodie', 'Vest', 'High heels', 'Flip Flops',
         'Sandals', "Long Coat", 'Tracking Pants', 'Tank Top', 'Singlet', 'Boots', 'Shorts', 'Sweat Pants',
         'Polo Shirt', 'Trousers', 'Dress', 'T-Shirt', 'Mittens', 'Socks', 'Swimsuit', 'Trench Coat', 'Winter Coat',
         'Straw hat', 'Cap', 'Scarf', 'Long-sleeve Top', 'Trainers Shoes']

BRANDS = ['Alanic', '2XU', '361 Degrees', 'Acerbis', 'A.F.C.A', 'Adidas', 'Admiral', 'Air Jordan', 'Airness', 'Alanic',
          'AND1', 'ANTA', 'Ashworth', 'ASICS', 'Athleta', 'Athletic DNA', 'Atletica', 'Audimas', 'Avia',
          'BIKE Athletic Company', 'BLK', 'Brine', 'British Knights', 'Brooks', 'Bukta', 'Canterbury', 'Capelli Sport',
          'Carbrini', 'Castore', 'CCM', 'Champion', 'Champs', 'Classic Sportswear', 'Columbia', 'Converse',
          'Craft of Scandinavia', 'De Marchi', 'Deuter Sport', 'Diadora', 'Donnay', 'Dryworld', 'Duarig', 'Dunlop',
          'Dynamic', 'Ellesse', 'ERKE', 'Erima', 'Erreà', 'Everlast', 'Fabletics', 'Fairtex Gym', 'FBT', 'Fila',
          'Finta', 'Forward', 'FSS', 'Galvin Green', 'Geox', 'Gilbert', 'Givova', 'Gola', 'Grand', 'Gul',
          ' Gunn & Moore', 'Head', 'Hummel', 'Hunkemöller HKMX', 'Ivivva Athletica', 'Jako', 'Joma', 'K-Swiss',
          'K2 Sports', 'Kappa', 'Karhu', 'Kelme', 'Kempa', 'Keuka', 'Kickers', 'Kukri', 'Lacoste', 'Le Coq Sportif',
          'League', 'Li-Ning', 'Lonsdale', 'Looptworks', 'Lorna Jane', 'Lotto', 'Loudmouth Golf', 'Luanvi',
          'Lululemon Athletica', 'LUTA', 'Macron', 'Majestic', 'Athletic', 'Marathon', 'Maverik Lacrosse', 'Merooj',
          'Meyba', 'Mikasa', 'Mitchell & Ness', 'Mitre', 'Mizuno', 'Molten', 'Moncler', 'Mondetta', 'Munich', 'Musto',
          'Nanque', 'New Balance', 'New Era', 'Nike', 'Nivia', 'No Fear', 'Nomis', 'Olympikus', 'Onda', 'One Way',
          'ONeill', 'Outdoor Voices', 'Patrick', 'Peak', 'Pearl Izumi', 'Penalty', 'Performax', 'Ping', 'Pirma', 'Pony',
          'Prince', 'Prospecs', 'Puma', 'Quechua', 'Quick', 'Rab', 'Rapha', 'Raymond', 'Reebok', 'Regatta', 'Reusch',
          'Riddell', 'Russell Athletic', 'Rykä', 'Robey', 'Saeta', 'Salomon', 'Samurai', 'Santini SMS', 'Select',
          'Sergio Tacchini', 'Sherrin', 'Shimano', 'Signia', 'SIX5SIX', 'Skins', 'Skis', 'Rossignol', 'Slazenger',
          'Soffe', 'Sondico', 'Spalding', 'SPECS', 'Speedo', 'Sportika', 'Starbury', 'Starter', 'Steeden', 'Sting',
          'Sugino', 'Superga', 'Swix', 'TaylorMade', 'The Game', 'Titleist', 'Tokaido', 'Topper', 'TYKA', 'Tyr',
          'Uhlsport', 'Umbro', 'Under Armour', 'Walon Sport', 'Warrior', 'Webb Ellis', 'Wilson', 'XBlades',
          'Xero Shoes', 'Xtep', 'Yonex', 'Zoke']

COLORS = ["Gray", "White", "Yellow", "Color", "Amaranth", "Amber", "Amethyst", "Apricot", "Aquamarine", "Azure",
          "Baby blue", "Beige", "Brick red", "Black", "Blue", "Blue-green", "Blue-violet", "Blush", "Bronze", "Brown",
          "Burgundy", "Byzantium", "California Red", "Carmine", "Cerise", "Cerulean", "Champagne", "Chartreuse green",
          "Chocolate", "Cobalt blue", "Coffee", "Copper", "Coral", "Crimson", "Cyan", "Desert sand", "Electric blue",
          "Emerald", "Erin", "Gold", "Gray", "Green", "Harlequin", "Indigo", "Ivory", "Jade", "Jungle green",
          "Lavender", "Lemon", "Lilac", "Lime", "Magenta", "Magenta rose", "Maroon", "Mauve", "Navy blue", "Ochre",
          "Olive", "Orange", "Orange-red", "Orchid", "Peach", "Pear", "Periwinkle", "Persian blue", "Pink", "Plum",
          "Prussian blue", "Puce", "Purple", "Raspberry", "Red", "Red-violet", "Rose", "Ruby", "Salmon", "Sangria",
          "Sapphire", "Scarlet", "Silver", "Slate gray", "Spring bud", "Spring green", "Tan", "Taupe", "Teal",
          "Turquoise", "Ultramarine", "Violet", "Viridian", "White", "Yellow", "Lime", "view", "talk", "Web colors",
          "Pink", "Crimson", "Red", "Maroon", "Brown", "Misty Rose", "Salmon", "Coral", "Orange-Red", "Chocolate",
          "Orange", "Gold", "Ivory", "Yellow", "Olive", "Yellow-Green", "Lawn green", "Chartreuse", "Lime", "Green",
          "Spring green", "Aquamarine", "Turquoise", "Azure", "Aqua/Cyan", "Teal", "Lavender", "Blue", "Navy",
          "Blue-Violet", "Indigo", "Dark Violet", "Plum", "Magenta", "Purple", "Red-Violet", "Tan", "Beige",
          "Slate gray", "Dark Slate Gray", "White", "White Smoke", "Light Gray", "Silver", "Dark Gray", "Gray",
          "Dim Gray", "Black",]
