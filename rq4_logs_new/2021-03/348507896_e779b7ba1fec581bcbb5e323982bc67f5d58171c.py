



#######IMPORTS#######

from global_vars import *

#######FUNCTION DEFINITIONS#########
def add_bbl(row):
    # print(row['Address'][0])
    try:
        if row['Address'][0].split(" ")[1][:-1].isnumeric() or row['Address'][0].split(" ")[1].isnumeric():
            mylist = row['Address'][0].split(" ")
            mylist[1] = mylist[0] + "-" + mylist[1]
            del mylist[0]
            row['Address'][0] = " ".join(mylist)
            # print(" ".join(mylist))
    except:
        pass
        
    inject = row['Address'][0] + " " + " ".join(row['Address'][1].split(" ")[: -2])
    try:
        response = requests.get("https://api.cityofnewyork.us/geoclient/v1/search.json?input="+ inject +"&app_id=d4aa601d&app_key=f75348e4baa7754836afb55dc9b6363d")
        decoded = response.content.decode("utf-8")
        json_loaded = json.loads(decoded)
        row["BBL"]=json_loaded['results'][0]['response']['bbl']
        # print("BBL " + str(json_loaded['results'][0]['response']['bbl']))
    except:
        # print(inject)
        row["BBL"]=""

    return row

def begin_process():
    barber_file_path = LOCAL_LOCUS_PATH + "data/whole/aca_license_center/barber.csv"
    df_barber = pd.read_csv(barber_file_path, encoding='utf-8')

    df_barber["License Number"] = df_barber["License Number"].apply(lambda x : x.replace(" ",""))
    df_barber["License Number"] = df_barber["License Number"].astype(str)
    df_barber["Name"] = df_barber["Name"].astype(str)

    df_barber["Name"]=df_barber["Name"].apply(lambda x:x.replace(u'\xa0', u' '))
    df_barber["Address"]=df_barber["Address"].apply(lambda x:x.replace(u'\xa0', u' '))

    df_barber["Address"] = df_barber["Address"].apply(lambda x : x.split("                "))

    print(df_barber["Address"])

    df_barber = df_barber.apply(lambda x : add_bbl(x), axis=1)
# 
    cleaned_file_path = LOCAL_LOCUS_PATH + "data/whole/aca_license_center/barber_update_0.csv"
    df_barber.to_csv(cleaned_file_path, index=False, quoting=csv.QUOTE_NONNUMERIC)

if __name__ == "__main__":
    begin_process()