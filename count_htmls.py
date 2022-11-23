import os
import json
import hydra
import boto3
import hydra
from tqdm.auto import tqdm
from urllib.parse import unquote
from omegaconf import OmegaConf, DictConfig



@hydra.main(config_path='.', config_name='config', version_base='1.2')
def main(cfg: DictConfig):
    print(OmegaConf.to_yaml(cfg))
    s3_client = boto3.client('s3', region_name=cfg.aws_region)

    paginator = s3_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=cfg.name)

    cnt = 0
    for page in response_iterator:
        cnt += len(page['Contents'])
        
    print(f'Number of htmls: {cnt}')


if __name__ == '__main__':
    main()