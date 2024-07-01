from torchvision import datasets, transforms

def get_dataset():
    # Transformaciones de los datos
    transform = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,))
    ])
    
    # Descargar el dataset MNIST
    dataset = datasets.MNIST(root='./data', train=True, download=True, transform=transform)
    return dataset