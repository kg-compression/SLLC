'''
This file is an old_version for compress and decompress, the latest version is compress.py in the same directory
'''
import os, sys, ljqpy, json
from collections import defaultdict
from tqdm import tqdm
from config import *
import time
import functions


name = 'wordnet_synset'
ruleselect = 3  # 选取第几类规则，1代表body长度为1，2代表body长度为2，3代表所有规则
fn = Config.data_path[name]
onumlim = Config.onum_rulethre[name][0]  # 三元组spo的o出现的次数，用于筛选三元组
rulethre = Config.onum_rulethre[name][1]  # 代表规则能够真正压缩的三元组个数，二者均用于剪枝加速
rule_count = 200  # 选取可用规则的条数
usefastcheck = True  # 是否使用fastcheck
GetSPO = functions.parse_triple_function(name)
vrules = []  # 可用规则
removed = set()  # 移除的三元组个数
exceptions = set()  # 用来存储规则特例
detailed_rules = {}  # 用来统计每个规则使用的次数
used_rules = []  # 用来统计压缩时实际使用的规则
supp1 = set()  # 用来统计压缩实际使用的规则所覆盖的三元组个数
supp = set()  # 用来统计压缩实际使用的规则所覆盖的三元组个数


# 根据置信度和supp数量获取到合适的规则
def get_valid_rules():
    if ruleselect == 3: valid_rules = ljqpy.LoadCSV('./rule/all_rules_rr_details_%s.txt' % name)
    if ruleselect == 1: valid_rules = ljqpy.LoadCSV('./rule/all_rules_rr_details_%s_1.txt' % name)
    if ruleselect == 2: valid_rules = ljqpy.LoadCSV('./rule/all_rules_rr_details_%s_2.txt' % name)
    used = set()
    for rule in valid_rules:
        # if len(vrules) == rule_count: break
        rr, supp, conf, cond, _, _ = rule
        # print(rr)
        supp, conf, cond = int(supp), float(conf), int(cond)
        if conf <= 0.5: continue
        if (supp * 2 - cond) < rulethre: continue
        # if supp < rulethre: continue
        # if rule_count > rulethre: break
        rr = eval(rr)
        if tuple(rr[-2:]) in used: continue
        used.add(tuple(rr[-2:]))
        if len(rr) == 4:  # 规则1和2
            cp, co, op, oo = rr
            if co == oo and cp == op: continue
            if co == 'x' and cp == op: continue
            vrules.append([cp, co, op, oo])
        else:  # 规则3
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
        ljqpy.WriteLine(ff, item)


def get_rules_exceptions():
    global used_rules, exceptions
    used_rules = ljqpy.LoadCSV('./temp/used_rules_details_%s.txt' % name)
    used_rules.reverse()
    exceptions.clear()
    for line in ljqpy.LoadCSVg('./temp/exceptions_details_%s.txt' % name):
        exceptions.add(tuple(line))
    if ('./temp/exceptions2_details_%s.txt' % name) in os.listdir():
        for line in ljqpy.LoadCSVg('./temp/exceptions2_details_%s.txt' % name):
            exceptions.add(tuple(line))
    print("压缩时使用的规则的数量:" + str(len(used_rules)))
    print("压缩时存储的规则特例：" + str(len(exceptions)))


def compressKB():
    global supp1
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

                        # 计算压缩时覆盖的三元组个数
                        if (len(rule) == 4):
                            if (rule[1] != 'x'):  # 规则1
                                condition = (head, rule[0], rule[1])
                                if condition in striples:
                                    supp1.add(condition)
                            else:  # 规则2
                                for triple in striples:
                                    if triple[2] == rule[0]:
                                        supp1.add(triple)
                        else:
                            condition_1 = (head, rule[0], rule[1])
                            condition_2 = (head, rule[2], rule[3])
                            if condition_1 in striples and condition_2 in striples:
                                supp1.add(condition_1)
                                supp1.add(condition_2)
                        removed.add(result)
                        striples.remove(result)

                    else:
                        if result not in removed:
                            exceptions.add(result)
            # 将压缩后的三元组写入文件，加上exception，每次写入一次防止太大
            write2file(striples, './temp/remained_details_%s.txt' % name)
            write2file(exceptions, './temp/exceptions_details_%s.txt' % name)
            # 置空方便处理下一个头实体
            striples.clear()
        striples.add((s, p, o))
        lasts = s
    # 将末尾的triple写入文件
    write2file(striples, './temp/remained_details_%s.txt' % name)
    # 将压缩用到的规则写入相应的文件
    simplify_rule = []
    print("压缩使用的规则数量：" + str(len(detailed_rules.keys())))
    for rule in vrules:
        if (detailed_rules.get(str(rule), 0) > 0):
            simplify_rule.append(rule)
    print("存储到文件中的规则数量:" + str(len(simplify_rule)))
    print('the length of cover in compress:', len(supp1))
    if os.path.exists('./temp/used_rules_details_%s.txt' % name):
        os.remove(r'./temp/used_rules_details_%s.txt' % name)
    write2file(simplify_rule, './temp/used_rules_details_%s.txt' % name)



# 根据压缩时的规则和规则特例实现解压，将KB恢复到初始的状态
def decompressKB():
    global files

    fndc = './temp/remained_details_%s.txt' % name
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
            write2file(striples, './temp/decompressKB_details_%s.txt' % name)
            # 置空方便处理下一个头实体
            striples.clear()
        striples.add((s, p, o))
        lasts = s
    # 将末尾的三元组写入文件
    write2file(striples, './temp/decompressKB_details_%s.txt' % name)
    for file in files.values(): file.close()
    files = {}


def decompressKB2():
    global files
    global supp
    fndc = './temp/remained_details_%s.txt' % name
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
                        # 将满足条件的三元组加入supp中
                        if (len(rule) == 4):
                            if (rule[1] != 'x'):  # 规则1
                                condition = (head, rule[0], rule[1])
                                if condition in striples:
                                    supp.add(condition)
                            else:
                                for triple in striples:
                                    if triple[2] == rule[0]:
                                        supp.add(triple)
                        elif len(striples) > 1:
                            condition_1 = (head, rule[0], rule[1])
                            condition_2 = (head, rule[2], rule[3])
                            if condition_1 in striples and condition_2 in striples:
                                supp.add(condition_1)
                                supp.add(condition_2)

            # 将解压后的三元组写入文件
            write2file(striples, './temp/decompressKB_details_%s.txt' % name)
            # 置空方便处理下一个头实体
            striples.clear()
        striples.add((s, p, o))
        lasts = s
    # 将末尾的三元组写入文件
    write2file(striples, './temp/decompressKB_details_%s.txt' % name)
    print('the length of cover:', len(supp))
    for file in files.values(): file.close()
    files = {}


def do_compress_KB():
    # 由于写入文件时添加到末尾，所以每次运行前需要删除
    if os.path.exists('./temp/remained_details_%s.txt' % name):
        os.remove(r'./temp/remained_details_%s.txt' % name)
    if os.path.exists('exceptions_details_%s.txt' % name):
        os.remove(r'./temp/exceptions_details_%s.txt' % name)
    if os.path.exists('./temp/exceptions2_details_%s.txt' % name):
        os.remove(r'./temp/exceptions2_details_%s.txt' % name)
    if os.path.exists('./temp/used_rules_details_%s.txt' % name):
        os.remove(r'./temp/used_rules_details_%s.txt' % name)
    get_valid_rules()
    compressKB()
    print('the compression process is complete')


def caculate_total_count():
    remain_count = 0
    exception_count = 0
    exception2_count = 0
    rule_count = 0
    for ii, zz in enumerate(ljqpy.LoadListg('./temp/remained_details_%s.txt' % name)):
        remain_count += 1
    for ii, zz in enumerate(ljqpy.LoadListg('./temp/exceptions_details_%s.txt' % name)):
        exception_count += 1
    for ii, zz in enumerate(ljqpy.LoadListg('./temp/exceptions2_details_%s.txt' % name)):
        exception2_count += 1
    for ii, zz in enumerate(ljqpy.LoadListg('./temp/used_rules_details_%s.txt' % name)):
        rule_count += 1
    count = remain_count + exception_count + exception2_count + rule_count
    print("第一次压缩后剩余的三元组个数：" + str(remain_count))
    print("第一次压缩后exceptions个数：" + str(exception_count))
    print("第一次解压缩后exceptions个数：" + str(exception2_count))
    print("第一次压缩后存储的规则个数：" + str(rule_count))
    print("The compression result of %s is : %d (%.4f)" % (name, count, count / lenold))


def do_decompress_KB(type):
    global files, lenold
    if (type == 1):
        # 第一次解压缩，扩展exceptions
        print("第一次解压开始")
        if os.path.exists('./temp/decompressKB_details_%s.txt' % name):
            os.remove(r'./temp/decompressKB_details_%s.txt' % name)
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
        for line in ljqpy.LoadCSVg('./temp/decompressKB_details_%s.txt' % name):
            lennew += 1
            line = tuple(line)
            if line not in old: new.add(line)
        print("第一次解压后的三元组个数：" + str(lennew))

        assert lennew >= lenold

        print("第一次解压产生的新的规则特例数量：" + str(len(new)))
        write2file(new, './temp/exceptions2_details_%s.txt' % name)
        print('第一次解压完成')
        for file in files.values(): file.close()
        files = {}

    if (type == 2):
        # 第二次解压
        print("第二次解压开始")
        if os.path.exists('./temp/decompressKB_details_%s.txt' % name):
            os.remove(r'./temp/decompressKB_details_%s.txt' % name)
        get_rules_exceptions()
        decompressKB2()
        count = 0
        for ii, zz in tqdm(enumerate(ljqpy.LoadListg('./temp/decompressKB_details_%s.txt' % name))):
            count += 1
        print("第二次解压后的三元组个数：" + str(count))
        print('第二次解压完成')


if __name__ == '__main__':
    # 第一次压缩
    start_time = time.time()
    print("压缩开始的时间：", start_time)
    do_compress_KB()
    print("压缩结束时所花费的时间：", time.time() - start_time)
    for file in files.values(): file.close()
    files = {}
    do_decompress_KB(1)
    print("从开始到第一次解压结束消耗的时间：", time.time() - start_time)
    caculate_total_count()
    do_decompress_KB(2)
    print("压缩解压消耗的总时间：", time.time() - start_time)
