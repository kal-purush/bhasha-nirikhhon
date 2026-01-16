import matplotlib.pyplot as plt
from pudb import set_trace
class UnNormalize(object):
    """ The purpose of this class is to display the augmented image"""
    def __init__(self, mean, std):
        self.mean = mean
        self.std = std

    def __call__(self, tensor):
        """
        Args:
            tensor (Tensor): Tensor image of size (C, H, W) to be normalized.
        Returns:
            Tensor: Normalized image.
        """
        for t, m, s in zip(tensor, self.mean, self.std):
            t.mul_(s).add_(m)
            # The normalize code -> t.sub_(m).div_(s)
        return tensor


def display_sample_images(train_loader, count=3):
    for i, (data, _) in enumerate(train_loader):
        unorm = UnNormalize(mean=(39.13,), std=(80.87,))
        unorm = UnNormalize(mean=(0.,), std=(1.,))
        img = unorm(data[0])
        plt.imshow(img[0].numpy())
        plt.show()
        if i == count-1:
            break