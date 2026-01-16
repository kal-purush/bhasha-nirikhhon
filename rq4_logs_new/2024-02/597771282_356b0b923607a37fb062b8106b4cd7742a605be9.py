import shutil
import os
import xmltodict

texture_struct = {
    "arial":  {"instances":
               [["14_800", 16], ["14_1024", 16], ["14_1600", 16],
                ["21_800", 16], ["21_1024", 16], ["21_1600", 23]],
               "filename": "ui_font_arial_"},
    "graff":  {"instances":
               [["19_800", 16], ["19_1024", 16], ["19_1600", 26],
                ["22_800", 16], ["22_1024", 16], ["22_1600", 26],
                ["32_800", 23], ["32_1024", 30], ["32_1600", 43],
                ["40_800", 31], ["40_1024", 38], ["40_1600", 58],
                ["50_800", 57], ["50_1024", 49], ["50_1600", 74]],
               "filename": "ui_font_graff_"},
    "letter": {"instances":
               [["16_800", 16], ["16_1024", 16], ["16_1600", 22],
                ["18_800", 16], ["18_1024", 16], ["18_1600", 23],
                ["25_800", 22], ["25_1024", 26], ["25_1600", 38]],
               "filename": "ui_font_letter_"},
}

family_letter = "Open Sans Condensed"
family_graff = "ELEGANT TYPEWRITER"

def make_template(template, family, fontsize, output):
    if family != "graff":
        temp = template.replace("$fontname", family_letter)
    else:
        temp = template.replace("$fontname", family_graff)
    temp = temp.replace("$fontsize", str(fontsize+4))

    if fontsize <= 20:
        temp = temp.replace("$w", "256")
        temp = temp.replace("$h", "256")
    elif fontsize > 20 and fontsize <=40:
        temp = temp.replace("$w", "512")
        temp = temp.replace("$h", "512")
    elif fontsize > 40:
        temp = temp.replace("$w", "1024")
        temp = temp.replace("$h", "1024")
    
    with open(output, "w") as output:
        output.write(temp)


with open("./template_conf.bmfc") as file:
    template = file.read()
    for family in texture_struct:
        for instance in texture_struct[family]["instances"]:
            fontsize = instance[1]
            output = texture_struct[family]["filename"]+instance[0]+".bmfc"
            make_template(template, family, fontsize, output)

for file in os.listdir("./"):
    if file.endswith(".bmfc") and file != "template_conf.bmfc":
        os.system(f"bmfont64.exe -c {file} -o {file.replace(".bmfc", ".fnt")}")

os.system("cls")
print("Cleaning up...")
for file in os.listdir("./"):
    if file != "template_conf.bmfc" and file.endswith(".bmfc"):
        os.remove(file)

for file in os.listdir("./"):
    if file.endswith(".fnt"):
        with open(file) as xmlFile:
            ghost_struct = [
                {"@id": str(k), "@x": "0", "@y": "0", "@width": "0", "@height": "0"}
                for k in range(256)
            ]
            data = xmltodict.parse(xmlFile.read())
            chars = data["font"]["chars"]["char"]

            ini = (
                "[width_correction]\nvalue = -1.0\n\n[symbol_coords]\nheight = "
                + data["font"]["info"]["@size"]
                + "\n"
            )

            for i in ghost_struct:
                # cycle thru available chars
                for j in chars:
                    if i["@id"] == j["@id"]:
                        i["@x"] = j["@x"]
                        i["@y"] = j["@y"]
                        i["@width"] = j["@width"]
                        i["@height"] = j["@height"]
                id = int(i["@id"])
                sid = i["@id"]
                x = int(i["@x"])
                y = int(i["@y"])
                wcoord = int(x + int(i["@width"]))
                hcoord = int(y + int(i["@height"]))

                if id < 10:
                    ini += f"00{sid} = {x}, {y}, {wcoord}, {hcoord}\n"
                elif id >= 10 and id < 100:
                    ini += f"0{sid} = {x}, {y}, {wcoord}, {hcoord}\n"
                elif id >= 100:
                    ini += f"{sid} = {x}, {y}, {wcoord}, {hcoord}\n"

            f = open(os.path.splitext(file)[0] + ".ini", "w")
            f.write(ini)
            f.close()

for file in os.listdir("./"):
    if file.endswith(".fnt"):
        os.remove(file)
        continue

    name = os.path.splitext(file)[0]
    ext = os.path.splitext(file)[1]

    if ext == ".dds":
        os.rename(file, f"{name[:-2]}{ext}")


os.makedirs("gamedata/textures/ui", exist_ok=True)
for file in os.listdir("."):
    if file.endswith(".dds") or file.endswith(".ini"):
        shutil.move(file, "gamedata/textures/ui")
shutil.make_archive(
    "alternative_fonts",
    "zip",
    root_dir=".",
    base_dir="gamedata/textures/ui",
)
shutil.rmtree("gamedata")

print("Done.")