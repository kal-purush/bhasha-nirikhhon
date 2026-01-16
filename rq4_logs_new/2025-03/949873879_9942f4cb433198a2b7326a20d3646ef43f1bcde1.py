from PIL import Image
import os

# Create the directory if it doesn't exist
os.makedirs('youtube-master-extension', exist_ok=True)

# Open the icon file
img = Image.open('assets/app.ico')

# Save in different sizes
img.save('youtube-master-extension/icon128.png', 'PNG')
img.resize((48, 48)).save('youtube-master-extension/icon48.png', 'PNG')
img.resize((16, 16)).save('youtube-master-extension/icon16.png', 'PNG')

print("Icon files created successfully!") 