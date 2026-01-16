import pandas as pd

messages = pd.read_csv('../data/survey/messages.csv')

chain_selector = (messages.chain_name == 'splish') & (messages.game_name == 'between-category-game-a')
chain = messages.ix[chain_selector]

chain.sort('generation', inplace=True)

seed_message = chain.pop(chain.index[0])
nested_messages = {'seed': seed_message}
for i,message in chain.iterrows():
