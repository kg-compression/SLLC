# 存放KG数据文件路径，生成文件路径等配置信息
import json


class Config():
    # the directory of KB dataset
    data_path = {'probase': './dataset/pbdata_sorted.txt',
                 'dbpedia': './dataset/infobox_properties_en.ttlrdup.txt',
                 'dbpedia_core': './sorteddbpedia_core.txt',
                 'cndbpedia': './dataset/triples_cndb_sorted.txt',
                 'wordnet': './dataset/wndata_sorted.txt',
                 'wordnet_synset': './dataset/wnisa_synsets.txt',
                 'conceptnet': './dataset/conceptnet_spo_rdup.txt',
                 'mscg': './dataset/ms_concept_graph_sorted.txt',
                 'MT': './dataset/TermGraph.txt',
                 'freebase': './dataset/sortedfreebase.txt',
                 'fb15k': './dataset/sortedfb15k.txt',
                 'fb15k-237': './dataset/sortedfb15k-237.txt',
                 'fb5m': './dataset/sortedfb5m.txt',
                 'wc_partof': './dataset/sortedwc_partof.txt',
                 'yago': './dataset/sortedyago.txt',
                 'yago2': './dataset/sortedyago2.txt',
                 'yago3': 'E:/fengjianchuan/code/kb_compress_from_92/kb_compress/dataset/sortedyago3.txt',
                 'yago4': './dataset/sortedyago4.txt',
                 'jdk': 'E:/fengjianchuan/code/kb_compress_from_92/kb_compress/dataset/sortedjdk.txt',
                 }
    #
    # 前者代表三元组spo的o出现的次数，用于筛选三元组
    # 后者用于规则，代表规则能够真正压缩的三元组个数，二者均用于剪枝加速
    onum_rulethre = {
        'conceptnet': [0, 10],
        'wordnet_synset': [0, 2],
        'probase': [20, 20],
        'cndbpedia': [20, 50],
        'dbpedia': [0, 5],
        'mscg': [20, 20],
        'MT': [0, 20],
        'fb15k': [0, 20],
        'fb15k-237': [0, 20],
        'fb5m': [0, 20],
        'freebase': [0, 20],
        'yago': [0, 20],
        'yago2': [0, 50],
        'yago3': [0, 50],
        'yago4': [0, 50],
        'dbpedia_core': [0, 50],
        'jdk': [0, 20],
    }

    # the data load function for different KB dataset
    def parse_triple_function(name):
        if name in ['probase', 'wordnet', 'wordnet_synset', 'mscg']:
            def f(zz):
                z = zz.split('\t')
                return z[1], 'isA', z[0]

            GetSPO = f

        elif name == 'conceptnet':
            def f(zz):
                return zz.split('\t')

            GetSPO = f

        elif name in ['MT', 'yago', 'freebase', 'fb15k', 'fb5m', 'yago2', 'yago3', 'yago4', 'jdk', 'dbpedia_core']:
            def f(zz):
                z = zz.split('\t')
                return z[0], z[1], z[2]

            GetSPO = f

        else:
            def f(zz):
                if zz.startswith('{'):
                    xx = json.loads(zz)
                    s, p, o = (xx[x] for x in 'spo')
                else:
                    s, p, o = zz.split('\t')
                return s, p, o

            GetSPO = f
        return GetSPO