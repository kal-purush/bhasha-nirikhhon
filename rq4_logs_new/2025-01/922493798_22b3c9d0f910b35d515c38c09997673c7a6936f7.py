import torch
import torch.nn as nn

#Create UNet model
class ConvBlock(nn.Module):
    def __init__(self, in_channel, out_channel):
        super(ConvBlock, self).__init__()
        self.Conv1 = nn.Conv2d(in_channel, out_channel, kernel_size=3, stride=1, padding=1)
        self.BN1 = nn.BatchNorm2d(out_channel)
        self.Conv2 = nn.Conv2d(out_channel, out_channel, kernel_size=3, stride=1, padding=1)
        self.BN2 = nn.BatchNorm2d(out_channel)
        
    def forward(self, x):
        x = self.Conv1(x)
        x = self.BN1(x)
        x = self.Conv2(x)
        x = self.BN2(x)
        return x
    
class DownsamplingBlock(nn.Module):
    def __init__(self, in_channel, out_channel):
        super(DownsamplingBlock, self).__init__()
        self.Pooling = nn.MaxPool2d(kernel_size=2, stride=2)
        self.ConvBlock = ConvBlock(in_channel, out_channel)
    
    def forward(self, x):
        x = self.Pooling(x)
        x = self.ConvBlock(x)
        return x
    
class Bottleneck(nn.Module):
    def __init__(self, in_channel, out_channel):
        super(Bottleneck, self).__init__()
        self.Downsampling = DownsamplingBlock(in_channel, out_channel)
        self.ConvBlock = ConvBlock(out_channel,  out_channel)

    def forward(self,x):
        x = self.Downsampling(x)
        x = self.ConvBlock(x)
        return x

class UpsamplingBlock(nn.Module):
    def __init__(self, in_channel, out_channel):
        super(UpsamplingBlock, self).__init__()
        self.Upsampling = nn.ConvTranspose2d(in_channel, in_channel, kernel_size=2, stride=2)
        self.ConvBlock = ConvBlock(in_channel + out_channel, out_channel)
    
    def forward(self, x1, x2):
        x1 = self.Upsampling(x1)
        hdiff = x2.size(2) - x1.size(2)
        wdiff = x2.size(3) - x1.size(3)
        x1 = nn.functional.pad(x1, [wdiff // 2, wdiff - wdiff // 2, hdiff // 2, hdiff - hdiff // 2])
        x = torch.cat([x1, x2], dim=1) 
        x = self.ConvBlock(x)
        return x
    
class SubPixelBlock(nn.Module):
    def __init__(self, in_channel, out_channel, upscale_factor=2):
        super(SubPixelBlock, self).__init__()
        self.Conv = nn.Conv2d(in_channel, out_channel*(upscale_factor**2), kernel_size=3, stride=1)
        self.PS = nn.PixelShuffle(upscale_factor)

    def forward(self, x):
        x = self.Conv(x)
        x = self.PS(x)
        return x

class UNet(nn.Module):
    def __init__(self,in_channel, out_channel):
        super(UNet, self).__init__()
        self.InputLayer = nn.Conv2d(in_channel, 32, kernel_size=3, stride=1, padding=1)
        self.Encoder1 = ConvBlock(32, 32)
        self.Encoder2 = DownsamplingBlock(32, 64)
        self.Encoder3 = DownsamplingBlock(64, 128)
        self.Encoder4 = DownsamplingBlock(128, 256)

        self.Bottleneck = Bottleneck(256, 512)

        self.Decoder1 = UpsamplingBlock(512, 256)
        self.Decoder2 = UpsamplingBlock(256, 128)
        self.Decoder3 = UpsamplingBlock(128, 64)
        self.Decoder4 = UpsamplingBlock(64, 32)
        
        self.final1 = ConvBlock(32, 32)
        self.final2 = ConvBlock(32, 32)
        self.final = nn.Conv2d(32, out_channel, kernel_size=1)

    def forward(self, x):
        Input = self.InputLayer(x)
        enc1 = self.Encoder1(Input)
        enc2 = self.Encoder2(enc1)
        enc3 = self.Encoder3(enc2)
        enc4 = self.Encoder4(enc3)

        bottleneck = self.Bottleneck(enc4)

        dec1 = self.Decoder1(bottleneck, enc4)
        dec2 = self.Decoder2(dec1, enc3)
        dec3 = self.Decoder3(dec2, enc2)
        dec4 = self.Decoder4(dec3, enc1)

        fin1 = self.final1(dec4)
        fin2 = self.final2(fin1)
        Output = self.final(fin2)
        return Output