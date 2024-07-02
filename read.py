import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import transforms
from torch.utils.data import DataLoader, random_split, TensorDataset
import pandas as pd
import mlflow
import mlflow.pytorch

def get_dataset():
    from torchvision import datasets
    
    # Transformaciones de los datos
    transform = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,))
    ])
    
    # Descargar el dataset MNIST
    dataset = datasets.MNIST(root='./data', train=True, download=True, transform=transform)

    # Convertir a DataFrame
    data_list = []
    for i in range(len(dataset)):
        img, label = dataset[i]
        img = img.numpy().flatten()  # Aplanar la imagen
        data_list.append((img, label))
    
    df = pd.DataFrame(data_list, columns=["image", "label"])
    return df