import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset



class ClimateData(Dataset):
    """"""

    def __init__(self,path_database):
        """
        Args:

        """
        self.database=pd.read_csv(path_database)
        
    def __len__(self):
        return len(self.database)-1
    
    def __getitem__(self, idx):

        return self.load_item(idx)

    def load_item(self, idx):
        
        x=self.database.iloc[idx].to_numpy(dtype=np.float)
        y=self.database.iloc[idx+1].to_numpy(dtype=np.float)
        
        x=torch.from_numpy(x).float()
        y=torch.from_numpy(y).float()

    
        return x, y
