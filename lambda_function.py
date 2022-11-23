import sys
sys.path.append('src')

import re
import io
import os
import json
import boto3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, quote, unquote

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36'}


def split_url(url):
    parsed = urlparse(url)
    domain = f'{parsed.scheme}://{parsed.netloc}'
    remain = url.replace(domain, '')
    return domain, remain

def join_url(domain, remain):
    if remain in ['', '/']:
        return domain
    return f'{domain}/{remain}'
    
    
def url_to_fname(url):
    return quote(url, encoding='utf-8').replace('/', '-')
    

def clean_soup(soup, stop_tags=['script', 'style', 'symbol']):
    for tag in stop_tags:
        [t.extract() for t in soup.select(tag)]
    return soup


def extract_urls(soup, domain):
    cands = [a.get('href') for a in soup.find_all('a')]
    accepted = []
    for cand in cands:
        if cand is None:
            continue
        elif re.match('.*[(pdf)(jpg)(jpeg)(exe)(json)]$', cand):
            continue
        elif cand.startswith('http'):
            accepted.append(cand)
        elif cand.startswith('/'):
            accepted.append(f'{domain}{cand}')
            
    return accepted


def lambda_handler(event, context):
    s3_client = boto3.client('s3', aws_access_key_id=event['aws_access_key_id'], aws_secret_access_key=event['aws_secret_access_key'])
    sqs_client = boto3.client('sqs', aws_access_key_id=event['aws_access_key_id'], aws_secret_access_key=event['aws_secret_access_key'])
    
    url = event['url']
    domain, remain = split_url(url)
    url = join_url(domain, remain)
    
    try:
        req = requests.get(url, headers=HEADERS, timeout=10)
        html = req.text
        if len(html) < 10000:
            return {'msg': 'too short', 'url': url}
        
        soup = BeautifulSoup(html, 'html.parser')
        if soup.html is None:
            return {'msg': 'empty html', 'url': url}
        
        lang = soup.html.get('lang')
        if lang != 'ko':
            return {'msg': 'not korean', 'url': url}
        
        soup = clean_soup(soup)
        new_urls = extract_urls(soup, domain)
        for u in new_urls:
            sqs_client.send_message(QueueUrl=event['sqs_queue_url'], MessageBody=json.dumps({'url': u}))
        
        html = str(soup)
        fname = url_to_fname(url)
        s3_client.upload_fileobj(io.BytesIO(html.encode('utf-8')), 'korean-htmls', f'{fname}.html')
        
        return {'msg': 'done', 'url': url}
    
    except Exception as e:
        return {'msg': f'Error: {str(e)}', 'url': url}