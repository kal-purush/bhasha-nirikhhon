import os
import torch
import torch.nn as nn
import torch.optim as optim
import torchvision.transforms as transforms
import torchvision.models.video as models
from torchvision.models.video import R3D_18_Weights
from torch.utils.data import Dataset, DataLoader
import cv2
import numpy as np
from tqdm import tqdm
import gradio as gr

# Paths
DATASET_PATH = "D:\\Dataset"
REAL_DIRS = ["Celeb-real", "Youtube-real"]
FAKE_DIRS = ["Celeb-synthesis"]
CHECKPOINT_PATH = "checkpoint.pth"


# Video Dataset Class
class VideoDataset(Dataset):
    def __init__(self, root_dir, real_dirs, fake_dirs, transform=None):
        self.videos = []
        self.labels = []
        self.transform = transform

        for real_dir in real_dirs:
            real_path = os.path.join(root_dir, real_dir)
            if os.path.exists(real_path):
                for file in os.listdir(real_path):
                    self.videos.append(os.path.join(real_path, file))
                    self.labels.append(0)  # Label 0 for real

        for fake_dir in fake_dirs:
            fake_path = os.path.join(root_dir, fake_dir)
            if os.path.exists(fake_path):
                for file in os.listdir(fake_path):
                    self.videos.append(os.path.join(fake_path, file))
                    self.labels.append(1)  # Label 1 for fake

    def __len__(self):
        return len(self.videos)

    def __getitem__(self, idx):
        video_path = self.videos[idx]
        label = self.labels[idx]
        video_tensor = self.process_video(video_path)
        return video_tensor, label

    def process_video(self, video_path, num_frames=16):
        cap = cv2.VideoCapture(video_path)
        frames = []
        frame_count = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))

        interval = max(1, frame_count // num_frames)
        for i in range(num_frames):
            cap.set(cv2.CAP_PROP_POS_FRAMES, i * interval)
            ret, frame = cap.read()
            if not ret:
                break
            frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            frame = cv2.resize(frame, (112, 112))
            frames.append(frame)

        cap.release()

        if len(frames) < num_frames:
            frames += [frames[-1]] * (num_frames - len(frames))  # Padding

        frames = np.array(frames, dtype=np.float32) / 255.0
        frames = torch.tensor(frames).permute(3, 0, 1, 2)  # (C, T, H, W)
        return frames


# Model Definition
class DeepfakeDetector(nn.Module):
    def __init__(self):
        super(DeepfakeDetector, self).__init__()
        self.resnet = models.r3d_18(weights=R3D_18_Weights.DEFAULT)
        self.resnet.fc = nn.Linear(512, 1)  # Binary Classification

    def forward(self, x):
        return self.resnet(x)


# Training Function with Checkpointing and Progress Tracking
def train(model, train_loader, val_loader, epochs=10, lr=0.0001):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)
    criterion = nn.BCEWithLogitsLoss()
    optimizer = optim.Adam(model.parameters(), lr=lr)
    start_epoch = 0
    processed_videos = 0

    # Load checkpoint if exists
    if os.path.exists(CHECKPOINT_PATH):
        checkpoint = torch.load(CHECKPOINT_PATH, map_location=device)
        model.load_state_dict(checkpoint['model_state_dict'], strict=False)
        optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
        start_epoch = checkpoint.get('epoch', 0)
        processed_videos = checkpoint.get('processed_videos', 0)
        print(f"Checkpoint Loaded! Resuming from epoch {start_epoch + 1}, {processed_videos} videos processed")

    for epoch in range(start_epoch, epochs):
        model.train()
        loop = tqdm(train_loader, desc=f"Epoch {epoch + 1}/{epochs}")

        for batch_idx, (videos, labels) in enumerate(loop):
            videos, labels = videos.to(device), labels.float().to(device)
            optimizer.zero_grad()
            outputs = model(videos).squeeze()
            loss = criterion(outputs, labels)
            loss.backward()
            optimizer.step()

            processed_videos += len(videos)
            loop.set_postfix(loss=loss.item(), processed_videos=processed_videos)
            print(f"Processed {processed_videos} videos, processing batch {batch_idx + 1}")

            # Save checkpoint every 10 videos
            if processed_videos % 10 == 0:
                torch.save({
                    'epoch': epoch + 1,
                    'model_state_dict': model.state_dict(),
                    'optimizer_state_dict': optimizer.state_dict(),
                    'processed_videos': processed_videos
                }, CHECKPOINT_PATH)
                print(f"Checkpoint saved after {processed_videos} videos")


# Dataloader
dataset = VideoDataset(DATASET_PATH, REAL_DIRS, FAKE_DIRS)
train_size = int(0.8 * len(dataset))
val_size = len(dataset) - train_size
train_dataset, val_dataset = torch.utils.data.random_split(dataset, [train_size, val_size])
train_loader = DataLoader(train_dataset, batch_size=4, shuffle=True)
val_loader = DataLoader(val_dataset, batch_size=4, shuffle=False)

# Train Model
model = DeepfakeDetector()
train(model, train_loader, val_loader, epochs=10, lr=0.0001)