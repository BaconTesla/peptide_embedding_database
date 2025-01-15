import os
import requests

def download_file(url, target_dir, filename):
    # 确保目标目录存在，如果不存在则创建
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    
    # 构建完整的文件路径
    file_path = os.path.join(target_dir, filename)
    
    try:
        response = requests.get(url)
        response.raise_for_status()  # 检查请求是否成功
        print(f'下载链接{url}')
        with open(file_path, 'wb') as file:
            file.write(response.content)
        print(f"文件已成功下载到：{file_path}")
    except requests.exceptions.RequestException as e:
        print(f"下载失败：{e}")

prefix = 'https://ftp.ebi.ac.uk/pub/databases/metagenomics/mgnify_genomes/'
suffix = 'genomes-all_metadata.tsv'

biomes = pd.read_csv('biomes_name.csv', index_col=0)
biomes = biomes.columns.to_list()

mgnify_genomes =  [b[7:].split('_') for b in biomes]
mgnify_genomes = [f'{m[0]}-{m[1]}/{m[2]}' if len(m) > 2 else '/'.join(m) for m in mgnify_genomes]

for i in range(0, len(biomes)):
    target_dir = os.path.join('Biome', biomes[i], 'metadata')
    url = os.path.join(prefix, genomes, suffix)
    filename = suffix
    download_file(url, target_dir, filename)