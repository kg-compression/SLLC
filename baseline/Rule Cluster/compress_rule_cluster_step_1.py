'''
第一步，主要是为了区分conflict和 unconflict
这一步的作用和HRC中的第一步是一样的
'''

import os, sys, ljqpy, json
from collections import defaultdict
from tqdm import tqdm
import time
from random import shuffle

name = 'cndbpedia'
usefastcheck = True
use_shuffle = False  # 将规则随机顺序
# usefastcheck = False
ruleselect = 3
lenold = 0
onum_rulethre = {

    'cndbpedia': [20, 50],
    'dbpedia': [0, 5],
    'fb15k': [0, 20],
    'conceptnet': [0, 10],
    'yago': [0, 20],
    'probase': [20, 20],
    'wordnet_synset': [0, 2],
    'imdb': [0, 0],
    'jdk': [0, 20],

    'mscg': [20, 20],
    'MT': [0, 20],
    'fb15k-237': [0, 20],
    'fb5m': [0, 20],
    'freebase': [0, 20],
    'yago2': [0, 50],
    'yago3': [0, 50],
    'yago4': [0, 50],
    'dbpedia_core': [0, 50],
    'dbpedia_2017': [0, 50],
    'covid': [0, 0],

}

onumlim = onum_rulethre[name][0]
rulethre = onum_rulethre[name][1]
rule_count = 200

if name == 'probase':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/pbdata_sorted.txt'
elif name == 'dbpedia':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress\dataset/infobox_properties_en.ttlrdup.txt'
elif name == 'cndbpedia':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/triples_cndb_sorted.txt'
elif name == 'wordnet':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/wndata_sorted.txt'
elif name == 'wordnet_synset':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/wnisa_synsets.txt'
elif name == 'conceptnet':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/conceptnet_spo_rdup.txt'
elif name == 'mscg':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/ms_concept_graph_sorted.txt'
elif name == 'MT':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/TermGraph.txt'
elif name == 'yago':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/sortedyago.txt'
elif name == 'freebase':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/sortedfreebase.txt'
elif name == 'fb15k':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/sortedfb15k.txt'
elif name == 'fb15k-237':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/sortedfb15k-237.txt'
elif name == 'fb5m':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/sortedfb5m.txt'
elif name == 'wc_partof':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/sortedwc_partof.txt'
elif name == 'yago2':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/sortedyago2.txt'
elif name == 'dbpedia_core':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/sorteddbpedia_core.txt'
elif name == 'yago3':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/sortedyago3.txt'
elif name == 'yago4':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/sortedyago4.txt'
elif name == 'dbpedia_2017':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/sorteddbpedia_2017.txt'
elif name == 'imdb':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/domain_KG/IMDB/imdb_triples.txt'
elif name == 'covid':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/domain_KG/covid/covid_triples.txt'
elif name == 'jdk':
    fn = r'E:\fengjianchuan\code\kb_compress_from_92\kb_compress/dataset/sortedjdk.txt'

if name in ['probase', 'wordnet', 'wordnet_synset', 'mscg']:
    def f(zz):
        z = zz.split('\t')
        return z[1], 'isA', z[0]


    GetSPO = f
elif name == 'conceptnet':
    def f(zz):
        return zz.split('\t')


    GetSPO = f
elif name in ['MT', 'yago', 'freebase', 'fb15k', 'fb5m', 'wc_partof', 'yago2', 'yago3', 'yago4', 'jdk']:
    def f(zz):
        z = zz.split('\t')
        return z[0], z[1], z[2]


    GetSPO = f

elif name in ['dbpedia_core', 'dbpedia_live']:
    def f(zz):
        return zz.split(' ', 2)


    GetSPO = f
elif name in ['imdb', 'covid']:
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

vrules = []
removed = set()
exceptions = set()  # 用来存储规则特例
detailed_rules = {}  # 用来统计每个规则使用的次数
# remained = set()
used_rules = []


# 根据置信度和supp数量获取到合适的规则
def get_valid_rules(thre=0.8):
    if ruleselect == 3: valid_rules = ljqpy.LoadCSV('./result/%s/all_rules_rr_details_%s.txt' % (name, name))
    if ruleselect == 1: valid_rules = ljqpy.LoadCSV('./result/%s/all_rules_rr_details_%s_1.txt' % (name, name))
    if ruleselect == 2: valid_rules = ljqpy.LoadCSV('./result/%s/all_rules_rr_details_%s_2.txt' % (name, name))

    used = set()
    for rule in valid_rules:
        # if len(vrules) == rule_count: break
        rr, supp, conf, cond, _, _ = rule
        # print(rr)
        supp, conf, cond = int(supp), float(conf), int(cond)
        if conf <= thre: continue
        if (supp * 2 - cond) < rulethre: continue
        # if supp < rulethre: continue
        # if rule_count > rulethre: break #  考虑规则的个数，用于绘制不同数量规则，对压缩效果的曲线
        rr = eval(rr)
        if tuple(rr[-2:]) in used: continue
        used.add(tuple(rr[-2:]))
        if len(rr) == 4:  # 规则1和2
            cp, co, op, oo = rr
            if co == oo and cp == op: continue
            # if co == 'x': continue
            if co == 'x' and cp == op: continue
            vrules.append([cp, co, op, oo])
        else:  # 规则3
            # 只考虑了head 长度为 1 的规则
            continue
            cp, co, op, oo, dp, do = rr
            if (dp, do) in [(cp, co), (op, oo)]: continue
            vrules.append([cp, co, op, oo, dp, do])
    print("valid rules count:" + str(len(vrules)))


files = {}


def write2file(data, file):
    global files
    ff = files.get(file)
    if not ff:
        files[file] = ff = open(file, 'w', encoding='utf-8')
    for item in data:
        # ff.write(str(item) + '\n')
        ljqpy.WriteLine(ff, item)


def get_rules_exceptions():
    global used_rules, exceptions
    used_rules = ljqpy.LoadCSV('./result/%s/hrc_rules_details_%s.txt' % (name, name))
    used_rules.reverse()
    exceptions.clear()
    # for line in ljqpy.LoadCSVg('./result/%s/exceptions_details_%s.txt' % (name, name)):
    #     exceptions.add(tuple(line))
    # if ('./result/%s/exceptions2_details_%s.txt' % (name, name)) in os.listdir():
    if os.path.exists('./result/%s/exceptions2_details_%s.txt' % (name, name)):
        for line in ljqpy.LoadCSVg('./result/%s/exceptions2_details_%s.txt' % (name, name)):
            exceptions.add(tuple(line))

    print("压缩时使用的规则的数量:" + str(len(used_rules)))
    print("压缩时存储的规则特例：" + str(len(exceptions)))


def compressKB():
    global lenold
    '''
		originalKB-》compressed KG
	'''
    # 第一次遍历，获取三元组，并统计每一个object对象的出现次数
    onums = defaultdict(int)
    for ii, zz in tqdm(enumerate(ljqpy.LoadListg(fn))):
        s, p, o = GetSPO(zz)
        onums[o] += 1
        lenold += 1
    fastcheck = {}
    if usefastcheck:
        for i, rule in enumerate(vrules):
            if rule[1] == 'x':
                fastcheck.setdefault(rule[0], set()).add(i)
            else:
                fastcheck.setdefault((rule[0], rule[1]), set()).add(i)

    lasts = ''
    striples = set()
    #  根据规则去压缩知识图谱
    for ii, zz in tqdm(enumerate(ljqpy.LoadListg(fn))):
        s, p, o = GetSPO(zz)
        if s != lasts:
            removed = set()
            exceptions = set()
            conflicts = set()
            head = lasts
            if head == '/m/0105y2':
                print(head)
            skip = False
            etriples = [x for x in striples if onums[x[-1]] > onumlim]
            if len(etriples) < 2: skip = True
            if usefastcheck:
                fastset = set()
                for x in striples:
                    fastset |= fastcheck.get(x[1], set())
                    fastset |= fastcheck.get((x[1], x[2]), set())
                if len(fastset) == 0: skip = True
            for i, rule in enumerate(vrules):
                if skip or len(striples) < 2: break
                if usefastcheck and i not in fastset: continue
                cond = False
                condition_list = []
                result = (head, rule[-2], rule[-1])
                if (len(rule) == 4):  # 规则1和2
                    if rule[1] != 'x':
                        condition = (head, rule[0], rule[1])
                        condition_list.append(condition)
                        if condition in striples: cond = True
                    else:
                        spset = set(x[1] for x in striples)
                        if rule[0] in spset: cond = True
                else:
                    condition_1 = (head, rule[0], rule[1])
                    condition_2 = (head, rule[2], rule[3])
                    condition_list.append(condition_1)
                    condition_list.append(condition_2)
                    if condition_1 in striples and condition_2 in striples: cond = True

                if cond:
                    if result not in striples:

                        # if result not in removed:
                        for con in condition_list:
                            conflicts.add(con)
                    else:
                        detailed_rules[str(rule)] = detailed_rules.get(str(rule), 0) + 1

            # print(len(striples), len(conflicts))
            no_conflicts = striples - conflicts
            # 将压缩后的三元组写入文件，加上exception，每次写入一次防止太大
            # write2file(others, './result/%s/others_%s.txt' % (name, name))
            write2file(conflicts, './result/%s/conflicts_%s.txt' % (name, name))
            write2file(no_conflicts, './result/%s/no_conflicts_%s.txt' % (name, name))
            # 置空方便处理下一个头实体
            striples.clear()
        striples.add((s, p, o))
        lasts = s
    # 将末尾的triple写入文件
    write2file(striples, './result/%s/conflicts_%s.txt' % (name, name))
    # 将压缩用到的规则写入相应的文件
    simplify_rule = []
    print("压缩使用的规则数量：" + str(len(detailed_rules.keys())))
    for rule in vrules:
        if (detailed_rules.get(str(rule), 0) > 0):
            simplify_rule.append(rule)
    print("存储到文件中的规则数量:" + str(len(simplify_rule)))
    if os.path.exists('./result/%s/hrc_rules_details_%s.txt' % (name, name)):
        os.remove(r'./result/%s/hrc_rules_details_%s.txt' % (name, name))
    write2file(simplify_rule, './result/%s/hrc_rules_details_%s.txt' % (name, name))


def do_compress_KB():
    # 由于写入文件时添加到末尾，所以每次运行前需要删除
    if os.path.exists('./result/%s/remained_details_%s.txt' % (name, name)):
        os.remove(r'./result/%s/remained_details_%s.txt' % (name, name))
    if os.path.exists('./result/%s/exceptions_details_%s.txt' % (name, name)):
        os.remove(r'./result/%s/exceptions_details_%s.txt' % (name, name))
    if os.path.exists('./result/%s/exceptions2_details_%s.txt' % (name, name)):
        os.remove(r'./result/%s/exceptions2_details_%s.txt' % (name, name))
    if os.path.exists('./result/%s/used_rules_details_%s.txt' % (name, name)):
        os.remove(r'./result/%s/used_rules_details_%s.txt' % (name, name))
    get_valid_rules(thre=0.8)
    if use_shuffle:
        shuffle(vrules)
    compressKB()
    print('the compression process is complete')


def caculate_total_count():
    count = 0
    remain_count = 0
    exception_count = 0
    exception2_count = 0
    rule_count = 0
    for ii, zz in enumerate(ljqpy.LoadListg('./result/%s/remained_details_%s.txt' % (name, name))):
        remain_count += 1
    for ii, zz in enumerate(ljqpy.LoadListg('./result/%s/exceptions_details_%s.txt' % (name, name))):
        exception_count += 1
    for ii, zz in enumerate(ljqpy.LoadListg('./result/%s/exceptions2_details_%s.txt' % (name, name))):
        exception2_count += 1
    for ii, zz in enumerate(ljqpy.LoadListg('./result/%s/used_rules_details_%s.txt' % (name, name))):
        rule_count += 1
    count = remain_count + exception_count + exception2_count + rule_count
    print("第一次压缩后剩余的三元组个数：" + str(remain_count))
    print("第一次压缩后exceptions个数：" + str(exception_count))
    print("第一次解压缩后exceptions个数：" + str(exception2_count))
    print("第一次压缩后存储的规则个数：" + str(rule_count))
    print("The compression result of %s is : %d (%.4f)" % (name, count, count / lenold))


def caculate_total_count_new():
    remain_count = 0
    conflicts_count = 0
    no_conclicts_count = 0
    rule_count = 0
    # for ii, zz in tqdm(enumerate(ljqpy.LoadListg('./result/%s/remained_details_%s.txt' % (name, name)))):
    #     remain_count += 1
    for ii, zz in tqdm(enumerate(ljqpy.LoadListg('./result/%s/conflicts_%s.txt' % (name, name)))):
        conflicts_count += 1
    for ii, zz in enumerate(ljqpy.LoadListg('./result/%s/no_conflicts_%s.txt' % (name, name))):
        no_conclicts_count += 1
    for ii, zz in enumerate(ljqpy.LoadListg('./result/%s/used_rules_details_%s.txt' % (name, name))):
        rule_count += 1

    count = conflicts_count + no_conclicts_count
    print("第一次压缩后剩余的三元组个数：" + str(remain_count))
    print("conflicts  count：" + str(conflicts_count))
    print("no conflict count：" + str(no_conclicts_count))
    print("总的三元组个数：" + str(count))
    # print("The compression result of %s is : %d (%.4f)" % (name, count, count / lenold))


if __name__ == '__main__':
    # 第一次压缩
    start_time = time.time()
    print("************压缩开始的时间：", start_time)
    do_compress_KB()
    # caculate_total_count_new()
    print("************压缩结束时所花费的时间：", time.time() - start_time)
    #
    # for file in files.values(): file.close()
    # files = {}
    # do_decompress_KB(1)
    # print("************从开始到第一次解压结束消耗的时间：", time.time() - start_time)
    # # lenold = 75131839
    # caculate_total_count()
    # do_decompress_KB(2)
    # print("************压缩解压消耗的总时间：", time.time() - start_time)
