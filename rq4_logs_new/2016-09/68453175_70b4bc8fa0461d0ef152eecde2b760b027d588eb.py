# from numpy import genfromtxt
# csv_file = '/Users/tnosov/Documents/tim/hack_the_north/yuri.csv'

# my_data = genfromtxt(csv_file, delimiter='|')


# print (my_data)


import pandas as pd

csv_file = '/Users/tnosov/Documents/tim/hack_the_north/yuri.csv'

titles = ["title",	"link",	"year",	"citations",	"na",	"DOI",	"na",	"na",	"na",	"na",	"Description"]


df=pd.read_csv(csv_file, sep='|',names=titles)

filtered_csv = df[['title','DOI','year', 'link', 'citations', 'Description']]


results_html = filtered_csv.to_html()


results_html = results_html.encode('ascii', 'ignore').decode('ascii')
# results_html = results_html[0:3819] + results_html[3821:-1] 

# results_html = results_html[0:3819] + results_html[3821:-1] 


print(type(results_html))



print(results_html[3820])

text_file = open("results.html", "w")
text_file.write(results_html)
text_file.close()
