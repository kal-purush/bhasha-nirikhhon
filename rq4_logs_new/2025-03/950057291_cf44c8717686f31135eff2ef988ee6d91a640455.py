import torch
print(torch.cuda.is_available())  # Should return True if GPU is detected
print(torch.cuda.device_count())  # Should return the number of GPUs
print(torch.cuda.get_device_name(0))  # Should print GPU name

device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Device set to use: {device}")