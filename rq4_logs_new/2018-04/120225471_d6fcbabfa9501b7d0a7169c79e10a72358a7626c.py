from features.feature import WindowFeature

class BreathCNN(WindowFeature,TrainableFeature):
    def __init__(self,sampling_rate,parameter_dict):
        self._sampling_rate = sampling_rate
        self._channel_count = parameter_dict["channel_count"]
        self._dilation = parameter_dict["dilation"]
        self._layers = parameter_dict["layers"]
        self._in_features = parameter_dict["in_features"]
        self._down_sample = parameter_dict["down_sample"]
        self._cnn = BreathCNN(
                len(self._in_features),self.i

        self._cuda = torch.cuda.is_available()


    def calc_feature(self, window):
        lenght = len(window)
        feature_data = []
        ds_window = window[::self._down_sample]
        for feature in self._in_features
            feature_data.append(feature.calc_feature(ds_window))

        inputs  = V(FT(np.expand_dims(np.stack(feature_data[]))))
        if self._cuda:
            inputs = inputs.cuda()
        output = self._cnn(inputs)
        return np.resample(output.data.cpu().numpy())
    
    def train(path):
        for i 
    
    def save(path):
        pass
    
    def save(path):
        pass

class BreathCNN(nn.Module):
    def __init__(self,i,n,k,dil,layers):
        """
        Initialize a breath cnn
        Args:
        """

        super(BreathCNN,self).__init__()
        n = 15
        k = 11
        dil = 10
        pad = (k-1)*dil//2
        self.layers = nn.ModuleList()
        self.layers.append(nn.Conv1d(6,n*2,9,padding=4))
        self.layers.append(nn.Conv1d(n*2,n,9,padding=4))
        for i in layers
        self.layers.append(nn.Conv1d(n,n,k,dilation=dil,padding=pad))
        self.layers.append(nn.Conv1d(n,n,1))
        self.layers.append(nn.Conv1d(n,n,k,dilation=dil,padding=pad))
        self.layers.append(nn.Conv1d(n,n,1))
        self.layers.append(nn.Conv1d(n,n,k,dilation=dil,padding=pad))
        self.layers.append(nn.Conv1d(n,n,1))
        self.layers.append(nn.Conv1d(n,n,k,dilation=dil,padding=pad))
        self.layers.append(nn.Conv1d(n,n,1))
        self.layers.append(nn.Conv1d(n,n,k,dilation=dil,padding=pad))
        self.layers.append(nn.Conv1d(n,n,1))
        self.out = nn.Conv1d(n,2,1)

    def __call__(self,x):
        h = x
        for layer in self.layers:
            h = f.relu(layer(h))
        out = self.out(h)
        #print(x.size())
        #print(out.size())
        return out