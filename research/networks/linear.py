import torch.nn as nn
import torch


class Linear(nn.Module):

    def __init__(self):
        """
        Initialize Linear.

        Parameters
        ----------
        
        None
        """

        super(Linear, self).__init__()
        self.fc_1 = nn.Linear(4, 256)
        self.relu_1 = torch.nn.ReLU()
        self.fc_2 = nn.Linear(256,4)

        

    def forward(self, x):
        
        x=self.fc_1(x)
        x = self.relu_1(x)
        x = self.fc_2(x)
      
        return x

