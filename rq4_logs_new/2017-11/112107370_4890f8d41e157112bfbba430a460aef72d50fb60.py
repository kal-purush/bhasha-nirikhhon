import pandas as pd
import matplotlib.pyplot as plt
import gmplot

def get_plot(hr=-1):
    input_file = "maydata.csv"
    df = pd.DataFrame.from_csv(input_file, sep='\t')
    df.Day = df.Day.astype(int)
    df.Hour = df.Hour.astype(int)

    df = df.loc[(df['Hour'] == hr)]

    #for elm in df['Lat'].tolist():
    #    print elm
    lats = []
    lons = []
    for index, row in df.iterrows():
        #print row
        for i in range(int(row['count'])):
            lats.append(float(row['Lat']))
            lons.append(float(row['Lon']))

    gmap = gmplot.GoogleMapPlotter(lats[0], lons[0], 18)
    gmap.scatter(lats, lons, 'green', size=50, marker=False)
    #gmap.heatmap(lats,lons)

    gmap.draw("static/thismap.html")


if __name__=="__main__":
    get_plot(5)
    
    #plt.plot(df['count'].tolist())
    #plt.ylabel('pickup count')
    #plt.show()
#print df['count'].tolist()