power_supply = ['power_supply', 'Voedingen']
CPU = ['CPU', 'processors']
case = ['case', 'behuizing', 'Behuizingen']
cooler = ['cooler', 'CPU-koelers', 'Casefans']
dvd = ['dvd', 'Optische drives']
video_card = ['video_card', 'grafische kaarten', 'Video kaarten']
hard_drive = ['hard_drive', "Harddisks/SSD's"]
memory = ['memory', 'RAM', 'Geheugenmodules']
motherboard = ['motherboard', 'Moederborden']
categories = [power_supply, CPU, case, cooler, dvd, video_card,
              hard_drive, memory, motherboard]


def translate_category(name):
    for category_names in categories:
        if name in category_names:
            return category_names[0]
    return None