import argparse
import torch
import yaml
from tqdm import tqdm

from networks.linear import Linear as Linear
import dataset as dataset



def _train(params,model,train_loader,criterion,optimizer):
    for epoch in range(0, params["training"]["epochs"]):
    
        print('Epoch {}, lr {}'.format(
            epoch, optimizer.param_groups[0]['lr']))
        
        accumulated_epoch_l2_loss = 0
        model.train()

        for batch_index, batch in enumerate(tqdm(train_loader)):
            model.zero_grad()
            x, y = batch
            x = x.to(params["training"]["device"])
            y = y.to(params["training"]["device"])
            y_pred = model(x)
            loss = criterion(y, y_pred)
            loss.backward()
            optimizer.step()
            accumulated_epoch_l2_loss += loss.item()
            iteration = epoch * len(train_loader) + batch_index
            print('Iteration Number: {}, Train MSE: {} '.format(iteration ,loss.item()))

#Todo create evaluation function

def main(yaml_path):
    with open(yaml_path) as file:
        params = yaml.load(file, yaml.FullLoader)
    assert params["action"] in ["train", "test"]


    model = Linear()
    train_data = dataset.ClimateData(params['model']['path_data_base'])

    train_loader = torch.utils.data.DataLoader(train_data, batch_size=params["training"]["batch_size"],
                                               shuffle=True, num_workers=params["training"]["num_workers"])

    optimizer = torch.optim.Adam(
        model.parameters(), lr=params["training"]["learning_rate"])
    criterion = torch.nn.MSELoss(reduction="mean")

    if params["action"] == "train":
        
        _train(params, model, train_loader, criterion, optimizer)

    elif params["action"] == "test":
        raise NotImplementedError("Test action not implemented.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--yaml-path", '-yp', default='params.yml', help="path to read the yaml file")

    args = parser.parse_args()
    yaml_path = args.yaml_path

    main(yaml_path)
