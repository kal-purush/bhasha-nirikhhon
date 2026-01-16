import math

def calc(chest_position, phone_position):

    spisok_chest = chest_position.split('|')
    spisok_phone = phone_position.split('|')

    lat_chest = float(spisok_chest[0])
    lon_chest = float(spisok_chest[1])

    lat_phone = float(spisok_phone[0])
    lon_phone = float(spisok_phone[1])

    dist = 6372795 * (2 * math.asin(math.sqrt(math.sin((lon_phone - lat_phone) / 2) ** 2 + math.cos(lat_phone) * math.cos(lon_phone) * math.sin((lon_chest - lat_chest) / 2) ** 2)))

#    if dist > 1000:
#        dist = dist /1000

    return int(dist)