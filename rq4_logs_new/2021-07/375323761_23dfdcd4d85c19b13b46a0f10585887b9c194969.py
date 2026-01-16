import torch


class Projection:
    def __init__(self, epsilon, clip_min=0., clip_max=1.):
        self.epsilon = epsilon
        self.clip_min = clip_min
        self.clip_max = clip_max

    def __call__(self, x, x_ref):
        return x.clamp(self.clip_min, self.clip_max)


class LinfProjection(Projection):
    def __init__(self, epsilon=0.3, clip_min=0., clip_max=1.):
        super().__init__(epsilon, clip_min, clip_max)

    def __call__(self, x, x_ref):
        x = torch.max(torch.min(x, x_ref + self.epsilon), x_ref - self.epsilon)
        return x.clamp(self.clip_min, self.clip_max)