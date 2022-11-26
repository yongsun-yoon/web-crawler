import os
import json
import hydra
import boto3
import hydra
from tqdm.auto import tqdm
from urllib.parse import unquote
from omegaconf import OmegaConf, DictConfig

def fname_to_url(fname):
    return unquote(fname.replace('-', '/'))
    

def step(url, lambda_client, queue_url):
    payload = {
        "url": url,
        "aws_access_key_id": os.environ['AWS_ACCESS_KEY_ID'],
        "aws_secret_access_key": os.environ['AWS_SECRET_ACCESS_KEY'],
        "sqs_queue_url": queue_url
    }
    
    res = lambda_client.invoke(
        FunctionName = 'CrawlFunc',
        InvocationType = 'Event',
        Payload = json.dumps(payload),
    )

    return res


@hydra.main(config_path='.', config_name='config', version_base='1.2')
def main(cfg: DictConfig):
    print(OmegaConf.to_yaml(cfg))

    lambda_client = boto3.client('lambda')
    sqs_client = boto3.client('sqs')
    queue_url = sqs_client.get_queue_url(QueueName=cfg.name)['QueueUrl']

    seen = open('seen.txt', 'r').read()
    seen = seen.strip().split('\n') if seen else []
    seen = set(seen)
    seen_fs = open('seen.txt', 'a')
    
    for url in cfg.seed_urls:
        if url in seen: continue
        step(url, lambda_client, queue_url)
        seen.add(url)
        seen_fs.write(url + '\n')

    while True:
        msg = sqs_client.receive_message(QueueUrl=queue_url)
        msg = msg.get('Messages')
        if msg is None: continue

        body = json.loads(msg[0]['Body'])
        url = body['url']

        if url in seen: continue
        step(url, lambda_client, queue_url)
        seen.add(url)
        seen_fs.write(url + '\n')

        if len(seen) % 1000 == 0:
            print(f'{len(seen):07d} url seen')


    

if __name__ == '__main__':
    main()