'''
# 第一步，对unconflict进行压缩
# 计算时出现大于1的置信度，是因为support对count做了筛选，但是conf时是对所有三元组做的压缩
这里cluster由于topk限制，我们只选择前20%的规则
'''
import os, sys, ljqpy, json
from collections import defaultdict
from tqdm import tqdm
import time
from random import shuffle

name = 'yago'
usefastcheck = True
use_shuffle = False # 将规则随机顺序
# usefastcheck = False
ruleselect = 3

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
# rule_count = 200

fn = './result/%s/no_conflicts_%s.txt' % (name, name)


def f(zz):
    z = zz.split('\t')
    return z

GetSPO = f


vrules = []
removed = set()
exceptions = set()  # 用来存储规则特例
detailed_rules = {}  # 用来统计每个规则使用的次数
# remained = set()
used_rules = []


# 根据置信度和supp数量获取到合适的规则
def get_valid_rules(ratio=0.6):
    global vrules
    vrules = ljqpy.LoadCSV('./result/%s/hrc_rules_details_%s.txt' % (name, name))
    vrules = vrules[:int(len(vrules)*ratio)]
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
    used_rules = ljqpy.LoadCSV('./result/%s/used_rules_details_%s.txt' % (name, name))
    used_rules.reverse()
    exceptions.clear()
    for line in ljqpy.LoadCSVg('./result/%s/exceptions_details_%s.txt' % (name, name)):
        exceptions.add(tuple(line))
    # if ('./result/%s/exceptions2_details_%s.txt' % (name, name)) in os.listdir():
    if os.path.exists('./result/%s/exceptions2_details_%s.txt' % (name, name)):
        for line in ljqpy.LoadCSVg('./result/%s/exceptions2_details_%s.txt' % (name, name)):
            exceptions.add(tuple(line))

    print("压缩时使用的规则的数量:" + str(len(used_rules)))
    print("压缩时存储的规则特例：" + str(len(exceptions)))


def compressKB():

    '''
		originalKB-》compressed KG
	'''
    # 第一次遍历，获取三元组，并统计每一个object对象的出现次数
    onums = defaultdict(int)
    for ii, zz in tqdm(enumerate(ljqpy.LoadListg(fn))):
        s, p, o = GetSPO(zz)
        onums[o] += 1
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
            head = lasts
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
                result = (head, rule[-2], rule[-1])
                if (len(rule) == 4):  # 规则1和2
                    if rule[1] != 'x':
                        condition = (head, rule[0], rule[1])
                        if condition in striples: cond = True
                    else:
                        spset = set(x[1] for x in striples)
                        if rule[0] in spset: cond = True
                else:
                    condition_1 = (head, rule[0], rule[1])
                    condition_2 = (head, rule[2], rule[3])
                    if condition_1 in striples and condition_2 in striples: cond = True
                if cond:
                    if result in striples:
                        detailed_rules[str(rule)] = detailed_rules.get(str(rule), 0) + 1
                        removed.add(result)
                        striples.remove(result)
                    else:
                        if result not in removed:
                            exceptions.add(result)
            # 将压缩后的三元组写入文件，加上exception，每次写入一次防止太大
            write2file(striples, './result/%s/remained_details_%s.txt' % (name, name))
            write2file(exceptions, './result/%s/exceptions_details_%s.txt' % (name, name))
            # 置空方便处理下一个头实体
            striples.clear()
        striples.add((s, p, o))
        lasts = s
    # 将末尾的triple写入文件
    write2file(striples, './result/%s/remained_details_%s.txt' % (name, name))
    # 将压缩用到的规则写入相应的文件
    simplify_rule = []
    print("压缩使用的规则数量：" + str(len(detailed_rules.keys())))
    for rule in vrules:
        if (detailed_rules.get(str(rule), 0) > 0):
            simplify_rule.append(rule)
    print("存储到文件中的规则数量:" + str(len(simplify_rule)))
    if os.path.exists('./result/%s/used_rules_details_%s.txt' % (name, name)):
        os.remove(r'./result/%s/used_rules_details_%s.txt' % (name, name))
    write2file(simplify_rule, './result/%s/used_rules_details_%s.txt' % (name, name))


# ignore the last entity
# 根据压缩时的规则和规则特例实现解压，将KB恢复到初始的状态


def decompressKB():
    global files

    fndc = './result/%s/remained_details_%s.txt' % (name, name)
    lasts = ''
    striples = set()
    # 根据获取到的规则和规则特例，将KB解压为初始状态

    fastcheck = set()
    if usefastcheck:
        for rule in used_rules:
            if rule[1] == 'x':
                fastcheck.add(rule[0])
            else:
                fastcheck.add((rule[0], rule[1]))

    for ii, zz in tqdm(enumerate(ljqpy.LoadCSVg(fndc))):
        s, p, o = zz
        if s != lasts:
            # 循环遍历
            head = lasts  # 头实体
            skip = False
            if len(fastcheck) > 0:
                for x in striples:
                    if (x[1], x[2]) in fastcheck: break
                    if x[1] in fastcheck: break
                else:
                    skip = True
            for rule in used_rules:
                if skip: break
                cond = False
                result = (head, rule[-2], rule[-1])
                if (len(rule) == 4):
                    if (rule[1] != 'x'):  # 规则1
                        condition = (head, rule[0], rule[1])
                        if condition in striples: cond = True
                    else:
                        spset = set(x[1] for x in striples)
                        if rule[0] in spset: cond = True
                elif len(striples) > 1:
                    condition_1 = (head, rule[0], rule[1])
                    condition_2 = (head, rule[2], rule[3])
                    if condition_1 in striples and condition_2 in striples: cond = True
                if cond:
                    if result not in exceptions:
                        striples.add(result)

            # 将解压后的三元组写入文件
            write2file(striples, './result/%s/decompressKB_details_%s.txt' % (name, name))
            # 置空方便处理下一个头实体
            striples.clear()
        striples.add((s, p, o))
        lasts = s
    # 将末尾的三元组写入文件
    write2file(striples, './result/%s/decompressKB_details_%s.txt' % (name, name))
    for file in files.values(): file.close()
    files = {}


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
    get_valid_rules()
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
    conflict = 0
    len_dict = { 'conceptnet': 6019139, 'wordnet_synset': 89089, 'probase': 16279648,
                      'cndbpedia': 75131839, 'dbpedia': 50601483, 'mscg': 33377320, 'MT': [0, 20], 'fb15k': 483142,
                      'fb15k-237': 272155, 'fb5m': 7688234, 'freebase': 362243580, 'yago': 12430701,
                      'yago2': 96645460, 'yago3': 53842572, 'yago4': 62926625, 'dbpedia_core': 100343086,
                      'dbpedia_2017': 52680100, 'imdb': 43059022, 'covid': 8971, 'jdk': 2563900
    }

    for ii, zz in enumerate(ljqpy.LoadListg('./result/%s/remained_details_%s.txt' % (name, name))):
        remain_count += 1
    for ii, zz in enumerate(ljqpy.LoadListg('./result/%s/exceptions_details_%s.txt' % (name, name))):
        exception_count += 1
    for ii, zz in enumerate(ljqpy.LoadListg('./result/%s/exceptions2_details_%s.txt' % (name, name))):
        exception2_count += 1
    for ii, zz in enumerate(ljqpy.LoadListg('./result/%s/used_rules_details_%s.txt' % (name, name))):
        rule_count += 1
    for ii, zz in enumerate(ljqpy.LoadListg('./result/%s/conflicts_%s.txt' % (name, name))):
        conflict += 1
    count = remain_count + exception_count + exception2_count + rule_count + 2 * conflict
    print("第一次压缩后剩余的三元组个数：" + str(remain_count))
    print("第一次压缩后exceptions个数：" + str(exception_count))
    print("第一次解压缩后exceptions个数：" + str(exception2_count))
    print("第一次压缩后存储的规则个数：" + str(rule_count))
    print("The compression result of %s is : %d (%.4f)" % (name, count, count / len_dict[name]))
    print('分母为原始文件中的三元组个数: ', lenold,  conflict, lenold + conflict, len_dict[name])


def do_decompress_KB(type):
    global files, lenold
    if (type == 1):
        # 第一次解压缩，扩展exceptions
        print("第一次解压开始")
        if os.path.exists('./result/%s/decompressKB_details_%s.txt' % (name, name)):
            os.remove(r'./result/%s/decompressKB_details_%s.txt' % (name, name))
        get_rules_exceptions()
        decompressKB()
        # 原始的wordnet数据
        old = set()
        new = set()
        with open(fn, encoding="utf-8") as fin:
            for line in fin:
                old.add(tuple(GetSPO(line.strip())))
        print("原文件中的三元组个数：", len(old))
        lenold = len(old)
        lennew = 0
        for line in ljqpy.LoadCSVg('./result/%s/decompressKB_details_%s.txt' % (name, name)):
            lennew += 1
            line = tuple(line)
            if line not in old: new.add(line)
        print("第一次解压后的三元组个数：" + str(lennew))

        assert lennew >= lenold

        print("第一次解压产生的新的规则特例数量：" + str(len(new)))
        write2file(new, './result/%s/exceptions2_details_%s.txt' % (name, name))
        print('第一次解压完成')
        for file in files.values(): file.close()
        files = {}
    if (type == 2):
        # 第二次解压
        print("第二次解压开始")
        if os.path.exists('./result/%s/decompressKB_details_%s.txt' % (name, name)):
            os.remove(r'./result/%s/decompressKB_details_%s.txt' % (name, name))
        get_rules_exceptions()
        decompressKB()
        count = 0
        for ii, zz in tqdm(enumerate(ljqpy.LoadListg('./result/%s/decompressKB_details_%s.txt' % (name, name)))):
            count += 1
        print("第二次解压后的三元组个数：" + str(count))
        print('第二次解压完成')


if __name__ == '__main__':
    # 第一次压缩
    start_time = time.time()
    print("************压缩开始的时间：", start_time)
    do_compress_KB()
    print("************压缩结束时所花费的时间：", time.time() - start_time)
    for file in files.values(): file.close()
    files = {}
    do_decompress_KB(1)
    print("************从开始到第一次解压结束消耗的时间：", time.time() - start_time)
    # lenold = 75131839
    caculate_total_count()
    do_decompress_KB(2)
    print("************压缩解压消耗的总时间：", time.time() - start_time)
