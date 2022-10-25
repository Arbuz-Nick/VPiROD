import torch 

class ConvNet(torch.nn.Module):
    cfg = [32, "M", 64, 64, "M", 128, 128, "M"]
    
    def __init__(self, n_classes=10, use_batchnorm=False, dropout_p=0.0):
        '''
        :param int n_classes: Число выходных признаков
        :param bool use_batchnorm: Использовать ли батчнорм между свёрточными слоями
        :param float dropout_p: Вероятность обнуления активации слоем Dropout
        '''
        super().__init__()
        
        self.n_classes = n_classes
        
        ### your code here
        self.features = torch.nn.Sequential()
        self.features.append(torch.nn.Conv2d(3, self.cfg[0], kernel_size=3, padding=1))
        if use_batchnorm:
            self.features.append(torch.nn.BatchNorm2d(num_features=self.cfg[0]))
        self.features.append(torch.nn.ReLU())
                                 
        self.features.append(torch.nn.MaxPool2d(2))
                                 
        self.features.append(torch.nn.Conv2d(self.cfg[0], self.cfg[2], kernel_size=3, padding=1))
        if use_batchnorm:
            self.features.append(torch.nn.BatchNorm2d(num_features=self.cfg[2]))
        self.features.append(torch.nn.ReLU())
                                 
        self.features.append(torch.nn.Conv2d(self.cfg[2], self.cfg[3], kernel_size=3, padding=1))
        if use_batchnorm:
            self.features.append(torch.nn.BatchNorm2d(num_features=self.cfg[3]))
        self.features.append(torch.nn.ReLU())
                    
        self.features.append(torch.nn.MaxPool2d(2))
        
        self.features.append(torch.nn.Conv2d(self.cfg[3], self.cfg[5], kernel_size=3, padding=1))
        if use_batchnorm:
            self.features.append(torch.nn.BatchNorm2d(num_features=self.cfg[5]))
        self.features.append(torch.nn.ReLU())
        
        self.features.append(torch.nn.Conv2d(self.cfg[5], self.cfg[6], kernel_size=3, padding=1))
        if use_batchnorm:
            self.features.append(torch.nn.BatchNorm2d(num_features=self.cfg[6]))
        self.features.append(torch.nn.ReLU())
                                 
        self.features.append(torch.nn.MaxPool2d(2))
        
        
        self.avgpool = torch.nn.AdaptiveAvgPool2d(2)
        
        self.classifier = torch.nn.Sequential()
        self.classifier.append(torch.nn.Linear(128 * 2 * 2, 128))
        self.classifier.append(torch.nn.Dropout(p=dropout_p))
        self.classifier.append(torch.nn.Linear(128, 128))
        self.classifier.append(torch.nn.Dropout(p=dropout_p))
        self.classifier.append(torch.nn.Linear(128, 128))
        self.classifier.append(torch.nn.Dropout(p=dropout_p))
        self.classifier.append(torch.nn.Linear(128, n_classes))
        
    def forward(self, x: torch.Tensor) -> torch.Tensor:
        x = self.features(x)
        x = self.avgpool(x)
        return self.classifier(x)