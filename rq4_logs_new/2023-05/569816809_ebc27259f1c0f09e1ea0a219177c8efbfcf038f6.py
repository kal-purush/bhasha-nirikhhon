with open("en_us.json") as file:
    data = file.read()
    
final = "{\n"
for line in data.split('\n'):
    if "item.minecraft." in line or "block.minecraft." in line:
        final += line + '\n'
        print(f"Added to key: " + str(
            {line
            .strip()
            .replace('"',"")
            .replace(",","")
            .split(': ')[0]: 
            line
            .strip()
            .replace('"',"")
            .replace(",","")
            .split(': ')[1]}
        ))
final += "\n}"
with open("en_us-item_key.json", 'w') as file:
    file.write(final[::-1].replace(',','',1)[::-1])