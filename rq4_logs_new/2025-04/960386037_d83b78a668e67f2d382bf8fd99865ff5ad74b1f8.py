import pandas

game_sales_raw_data = pandas.read_csv("Video_Games_Sales_as_at_22_Dec_2016.csv")
game_sales_raw_data.head()
print(game_sales_raw_data.describe())