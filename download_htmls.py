import os
import hydra
import boto3
import hydra
from omegaconf import OmegaConf, DictConfig



@hydra.main(config_path='.', config_name='config', version_base='1.2')
def main(cfg: DictConfig):
    print(OmegaConf.to_yaml(cfg))
    s3_client = boto3.client('s3')

    paginator = s3_client.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=cfg.name)

    cnt = 0
    for page in response_iterator:
        for content in page['Contents']:
            try:
                key = content['Key']
                fpath = f'{cfg.datadir}/{key}'
                if not os.path.exists(fpath):
                    s3_client.download_file(cfg.name, key, fpath)
                cnt += 1
            
            except:
                pass
            
            if cnt % 1000 == 0:
                print(f'download htmls: {cnt}')


if __name__ == '__main__':
    main()