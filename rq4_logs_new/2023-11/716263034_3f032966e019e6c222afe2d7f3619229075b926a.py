# Import and starting
from settingsPacket import settings
from PIL import Image, ImageShow

set = open("E:\PyProjectsFiles\PixelMixer\Settings.json", 'r+')
img = Image.open("E:\PyProjectsFiles\PixelMixer\pack.png")

# Settings load
settings2 = list(set.read())

#settings.load()
settings.print()
#settings.backup("E:\PyProjectsFiles\PixelMixer\Settingsbackup.properties", 'r')


# Settings checking
# Поки що в файлі має бути 0 на початку щоб згенерувались налаштування за замовчуванням
# Інакше виб'є помилку (Вже виправлено)

if len(settings2) >= 1:
    if settings2[0] == "1":
        print("settings loaded")
    else:
        print("settings lost")
else:
    settings2.append('0')
    if settings2[0] == "1":
        print("settings loaded")
    else:
        print("settings lost")

#img.resize((Settings[0], Settings[0]))

#img.