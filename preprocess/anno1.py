import pandas as pd
import os
import numpy as np
from concurrent.futures import ProcessPoolExecutor
import argparse

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="Habitat and Taxnomic annotation")
    parser.add_argument('--id', required=True, help="Biome id.")
    return parser.parse_args()

def process_biome_length(biome, biome_name, length, meta, target_dir):
    """
    处理某个生境和长度的计算过程，写入文件。

    参数:
    biome : str
        生境名称
    biome_name : str
        生境名称（用于输出）
    length : int
        需要处理的蛋白质序列长度
    meta : pd.DataFrame
        生物信息数据
    first_index : int
        数据索引偏移量
    target_dir : str
        输出目录
    """
    # 定义结果文件路径
    file_path = os.path.join(target_dir, f'length_{length}.tsv')

    # 读取初始文件
    df = pd.read_csv(f'./MGnify_genomes/ProtT5_LSTM_{length}AA.csv', index_col=0, low_memory=False)
    df_biome = df[biome]

    # 寻找非 NaN 所在位置
    non_na_indices = np.where(df_biome.notna())[0]
    
    if len(non_na_indices) == 0:
        print(f'no annotation for ProtT5_LSTM_{length}AA.csv!')
        return
    
    # 初始化字典
    pep_dict = {
        'id': [],
        'genome': [],
        'biomes': [],
        'taxnomic': []
    }

    for i in non_na_indices:
        genome_info = df_biome.iloc[i].split(';')
        for info in genome_info:
            gid = info.split(':')[0]
            if gid.startswith('MGYG') or gid.endswith('.1'):
                pep_dict['id'].append(df_biome.index[i])
                if gid.endswith('.1'):
                    pep_dict['genome'].append(gid[:15])
                    pep_dict['taxnomic'].append(meta.loc[gid[:15]])
                else:
                    pep_dict['genome'].append(gid[:13])
                    pep_dict['taxnomic'].append(meta.loc[gid[:13]])
                pep_dict['biomes'].append(biome_name)
            else:
                continue
    # 保存结果到文件
    df_pep = pd.DataFrame(pep_dict)
    df_pep.to_csv(file_path, index=False, sep='\t')

def main(id):
    # 确保当前路径正确
    os.chdir('/home/jovyan/work/Habitat_Anno/')

    # 加载生境和元数据
    biomes = pd.read_csv('biomes_name.csv', index_col=0)
    biomes = biomes.columns.to_list()
    mgnify_genomes = [b[7:].split('_') for b in biomes]
    mgnify_genomes = [f'{m[0]}-{m[1]}/{m[2]}' if len(m) > 2 else '/'.join(m) for m in mgnify_genomes]

    biome = biomes[int(id)]
    print(f'计算生境{biome}')
    biome_name = mgnify_genomes[int(id)].split('/')[0]
    target_dir = f'./Biome/{biome}/data_by_length/'
    os.makedirs(target_dir, exist_ok=True)

    # 加载元数据
    meta = pd.read_csv(f'./Biome/{biome}/metadata/genomes-all_metadata.tsv', sep='\t', index_col = 0)
    meta = meta['Lineage']

    # 使用并行化处理每个长度
    with ProcessPoolExecutor() as executor:
        lengths = range(5, 101)
        tasks = [executor.submit(process_biome_length, biome, biome_name, length, meta, target_dir) for length in lengths]

        # 等待所有任务完成
        for task in tasks:
            task.result()

if __name__ == "__main__":
    # 解析命令行参数
    args = parse_args()
    main(args.id)