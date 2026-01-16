from model import Prediction

url = "google.com"
our_model = Prediction()
our_model.check_accuracy()
print(our_model.accuracy)

print(our_model.classify_website(url))