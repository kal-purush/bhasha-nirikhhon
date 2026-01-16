from orm import *
import peewee as pw
import datetime
import matplotlib.pyplot as plt

MARKET_ID = 3

db = pw.SqliteDatabase('example_dataset.db', pragmas={'foreign_keys': 1})
database_proxy.initialize(db)



def main():

    times = []
    prices = []
    volumes = []
    cum_vol = 0
    print('loading data')

    for trade in Trade.select().where(Trade.market_id == MARKET_ID):
        times.append(trade.time)
        prices.append(float(trade.price))
        cum_vol += float(trade.volume)
        volumes.append(cum_vol)

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()


    ax2.fill(times + [times[-1]], volumes + [0], facecolor='orange', alpha=0.5)
    ax2.plot(times, volumes, color='orange')

    ax1.plot(times, prices)


    plt.show()



    

if __name__=="__main__":
    main()