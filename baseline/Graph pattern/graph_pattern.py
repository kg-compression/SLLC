import copy
import os, sys, ljqpy, json
from collections import defaultdict
from tqdm import tqdm
import time
from random import shuffle

"""
识别graph_pattern, 然后用graph pattern去压缩KB
"""

onum_rulethre = {
    'conceptnet': [0, 10], 'wordnet_synset': [0, 2],
    'nc_wordnet_synset': [0, 2], 'probase': [20, 20],
    'cndbpedia': [20, 50], 'dbpedia': [0, 5],
    'mscg': [20, 20], 'MT': [0, 20],
    'fb15k': [0, 20], 'fb15k-237': [0, 20],
    'fb5m': [0, 20], 'freebase': [0, 20],
    'yago': [0, 20], 'yago2': [0, 50],
    'yago3': [0, 50], 'yago4': [0, 50],
    'dbpedia_core': [0, 50], 'dbpedia_2017': [0, 50],
    'imdb': [0, 0], 'covid': [0, 0], 'jdk': [0, 20]
}


def find_pattern(name):
    used = set()  # 被用过的pattern
    unused_rule = set()  # 没被用过的pattern
    patterns = []
    expected_compress_count = 0
    # step1 直接从 rule_support中去挖掘graph_pattern
    for x, y in ljqpy.LoadCSV(r'..\rules\rules_%s.txt' % name):
        p, o, p1, o1 = eval(x)
        # 如果该条pattern没有被使用过
        if (p, o) not in used and (p1, o1) not in used:
            patterns.append(str(x))
            expected_compress_count += int(y)
            used.add((p, o))
            used.add((p1, o1))

    print(f'预期可以压缩的三元组数量：', expected_compress_count)

    # 保存 graph_pattern
    ljqpy.SaveList(patterns,
                   r'..\dataset\Graph_pattern\pattern_%s.txt' % name)


def get_kb_filepath(name):
    if name == 'fb15k':
        fn = r'..\dataset\sortedfb15k.txt'
    


    return fn


def parse_kb(kb_name):
    if kb_name in ['probase', 'wordnet', 'wordnet_synset', 'mscg']:
        def f(zz):
            z = zz.split('\t')
            return z[1], 'isA', z[0]

        GetSPO = f
    elif kb_name == 'conceptnet':
        def f(zz):
            return zz.split('\t')

        GetSPO = f
    elif kb_name in ['MT', 'yago', 'freebase', 'fb15k', 'fb5m', 'wc_partof', 'yago2', 'yago3', 'yago4']:
        def f(zz):
            z = zz.split('\t')
            return z[0], z[1], z[2]

        GetSPO = f

    elif kb_name in ['dbpedia_core', 'dbpedia_live']:
        def f(zz):
            return zz.split(' ', 2)

        GetSPO = f
    elif kb_name in ['imdb', 'covid']:
        def f(zz):
            return eval(zz)

        GetSPO = f
    else:
        def f(zz):
            if zz.startswith('{'):
                xx = json.loads(zz)
                s, p, o = (xx[x] for x in 'spo')
            else:
                # print(zz)
                s, p, o = zz.split('\t')
            return s, p, o

        GetSPO = f

    return GetSPO


files = {}


def write2file(data, file):
    global files
    ff = files.get(file)
    if not ff:
        files[file] = ff = open(file, 'w', encoding='utf-8')
    for item in data:
        # ff.write(str(item) + '\n')
        ljqpy.WriteLine(ff, item)


# step2 根据挖掘出来的grapgh_pattern 用生成的graph pattern 替换原始的三元组
def compress(name):
    fn = get_kb_filepath(name)
    patterns = ljqpy.LoadList(
       r'..\dataset\Graph_pattern\pattern_%s.txt' % name)
    patterns = [eval(i) for i in patterns]
    GetSPO = parse_kb(name)

    compressed_count = 0
    used_pattern = set()
    lasts = ''
    striples = set()
    # 遍历三元组，找出其中的pattern
    for ii, zz in tqdm(enumerate(ljqpy.LoadListg(fn))):
        try:
            s, p, o = GetSPO(zz)
        except:
            continue
        if s != lasts:
            # 循环遍历
            for index, pattern in enumerate(patterns):
                a, b, c, d = pattern
                if (lasts, a, b) in striples and (lasts, c, d) in striples:
                    striples.remove((lasts, a, b))
                    striples.remove((lasts, c, d))
                    striples.add((lasts, 'pattern', index))
                    used_pattern.add(pattern)
            # 将压缩之后的三元组保存
            # compressed = compressed | striples
            compressed_count += len(striples)
            write2file(striples, f'./{name}_gp_compressed.txt')
            striples.clear()

        striples.add((s, p, o))
        lasts = s

    compressed_count += len(striples)
    write2file(striples, f'./{name}_gp_compressed.txt')
    ljqpy.SaveCSV(used_pattern, f'./{name}_used_pattern.txt')
    print(compressed_count, len(used_pattern))


def decompress(name):
    patterns = ljqpy.LoadList(
        r'..\Graph_pattern\pattern_%s.txt' % name)
    patterns = [eval(i) for i in patterns]
    GetSPO = parse_kb(name)
    compressed_fn = f'./{name}_gp_compressed.txt'

    lasts = ''
    striples = set()
    # 遍历三元组，找出其中的pattern
    for ii, zz in tqdm(enumerate(ljqpy.LoadListg(compressed_fn))):
        try:
            s, p, o = GetSPO(zz)
        except:
            continue
        if s != lasts:
            # 循环遍历
            striples_copy = copy.deepcopy(striples)
            for triple in striples_copy:
                s1, p1, o1 = triple
                if p1 == 'pattern':
                    p2, o2, p3, o3 = patterns[int(o1)]
                    striples.remove((s1, p1, o1))
                    striples.add((s1, p2, o2))
                    striples.add((s1, p3, o3))

            write2file(striples, f'./{name}_gp_decompressed.txt')
            striples.clear()
            striples_copy.clear()

        striples.add((s, p, o))
        lasts = s

    # 将压缩后的三元组写入文件，加上exception，每次写入一次防止太大
    write2file(striples, f'./{name}_gp_decompressed.txt')


if __name__ == '__main__':
    # name = 'fb15k'
    # ['fb15k', 'conceptnet', 'yago', 'probase', 'wordnet_synset', 'imdb', 'jdk']
    name_list = ['dbpedia']
    for name in name_list:
        # 挖掘graph pattern
        find_pattern(name)
        # 压缩
        compress(name)
        # 解压
        # decompress(name)
