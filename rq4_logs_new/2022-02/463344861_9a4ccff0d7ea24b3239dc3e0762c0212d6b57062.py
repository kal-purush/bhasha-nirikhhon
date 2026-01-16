########### CONFIG ##########
TOP_NUMBER_OF_STOCKS = 10



def getTopStocks(stocks, printOutput: bool = False):
    
    symbols = dict(sorted(stocks.items(), key=lambda item: item[1], reverse = True))
    topStocks = list(symbols.keys())[0:10]
        
    if printOutput:
        print(f"\n{TOP_NUMBER_OF_STOCKS} most mentioned Stocks: ")
    
    times = []
    top = []
    for stockName in topStocks:
        
        if printOutput:
            print(f"{stockName}: {symbols[stockName]}")
            
        times.append(symbols[stockName])
        top.append(f"{stockName}: {symbols[stockName]}")
        
    return symbols, topStocks