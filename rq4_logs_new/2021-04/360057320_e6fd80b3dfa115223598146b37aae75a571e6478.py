
import torch
from torchvision import datasets
from torchvision.transforms import ToTensor

from network import NeuralNetwork


device = "cuda" if torch.cuda.is_available() else "cpu"
print("Using {} device".format(device))

m = torch.load('model.pth')
print(m)

model = NeuralNetwork()
model.load_state_dict(m)
model.to(device)
print(model)
model.eval()

test_data = datasets.FashionMNIST(
    root="data",
    train=False,
    download=True,
    transform=ToTensor(),
)
classes = [
    "T-shirt/top",
    "Trouser",
    "Pullover",
    "Dress",
    "Coat",
    "Sandal",
    "Shirt",
    "Sneaker",
    "Bag",
    "Ankle boot",
]

x, y = test_data[100][0], test_data[100][1]

with torch.no_grad():
    pred = model(x.to(device))
    predicted, actual = classes[pred[0].argmax(0)], classes[y]
    print(f'Predicted: "{predicted}", Actual: "{actual}"')

model_ts = torch.jit.load("model.pt")
model_ts.to(device)
with torch.no_grad():
    pred = model_ts(x.to(device))
    predicted, actual = classes[pred[0].argmax(0)], classes[y]
    print(f'Predicted: "{predicted}", Actual: "{actual}"')

