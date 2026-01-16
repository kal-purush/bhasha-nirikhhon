import pickle

data = None

with open("../pickles/preprocessed_data.pkl","rb") as f:
    data = pickle.load(f)

print(data.columns)