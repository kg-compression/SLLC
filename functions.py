import os, sys, ljqpy, json
import time
from collections import defaultdict
from tqdm import tqdm
from config import *


def Rules1_Supp(name, fn, onum=0):
    '''
        (s, p, o) ->(s, p1, o1)
    '''

    onums = defaultdict(int)
    GetSPO = Config.parse_triple_function(name)
    for ii, zz in tqdm(enumerate(ljqpy.LoadListg(fn))):
        try:
            s, p, o = GetSPO(zz)
        except:
            continue
        onums[o] += 1

    supports = {}
    lasts = ''
    striples = set()
    for ii, zz in tqdm(enumerate(ljqpy.LoadListg(fn))):

        try:
            s, p, o = GetSPO(zz)
        except:
            continue
        if s != lasts:
            for _, b, c in striples:
                for _, d, e in striples:
                    if (b, c) == (d, e): continue  # 如果
                    supports[(b, c, d, e)] = supports.get((b, c, d, e), 0) + 1
            striples = set()
        # if onums[o] > 50:
        if onums[o] > onum:
            striples.add((s, p, o))
        lasts = s
        if len(supports) > 30000 and (ii % 1000000 == 0 or len(supports) > 4000000):
            print('make rule list ...', ii, len(supports))
            rules = ljqpy.FreqDict2List(supports)
            rules = [x for x in rules if x[1] > 1][:20000]
            ljqpy.SaveCSV(rules, './rule/%s/rules_1_%s.txt' % (name, name))
            supports = {x: y for x, y in rules}
    print('make rule list ...', ii, len(supports))
    rules = ljqpy.FreqDict2List(supports)
    rules = [x for x in rules if x[1] > 1][:20000]
    ljqpy.SaveCSV(rules, './rule/%s/rules_1_%s.txt' % (name, name))


def Rules1_Conf(name, fn):

    GetSPO = Config.parse_triple_function(name)
    rules = {eval(x): y for x, y in ljqpy.LoadCSV('./rule/%s/rules_1_%s.txt' % (name, name))}
    eles = {(x[0], x[1]) for x in rules}
    striples = []
    supps = defaultdict(int)
    conds = defaultdict(int)
    cases = {}
    lasts = ''
    allcount = 0

    def make_rule_details():
        print('make rule details ...', ii)
        rdetails = []
        for c, supp in supps.items():  # supps.item: (p,o,p1,o1):24
            cc = (c[0], c[1])  # (p,o)
            cnum = conds[cc]
            conf = supp / cnum
            rdetails.append((c, supp, conf, cnum, cases[c], cases[cc]))
        rdetails.sort(key=lambda x: (-x[2], -x[1]))
        ljqpy.SaveCSV(rdetails, './rule/%s/rules_1_%s.txt' % (name, name))

    for ii, zz in tqdm(enumerate(ljqpy.LoadListg(fn))):
        try:
            s, p, o = GetSPO(zz)
        except:
            continue
        allcount += 1
        if s != lasts:
            for a, b, c in striples:
                if (b, c) not in eles: continue
                conds[(b, c)] += 1
                if len(cases.get((b, c), [])) < 20: cases.setdefault((b, c), []).append(a)
                for aa, d, e in striples:
                    assert aa == a
                    if (b, c, d, e) not in rules: continue
                    supps[(b, c, d, e)] += 1
                    if len(cases.get((b, c, d, e), [])) < 10:
                        cases.setdefault((b, c, d, e), []).append(a)
            striples = []

        if ii % 1000000 == 0: make_rule_details()
        striples.append((s, p, o))
        lasts = s
    make_rule_details()
    print('allcount:', allcount)


def Rules2_Supp(name, fn, onum=0):
    '''
        (s, p, x) ->(s, p1, o1)
    '''

    onums = defaultdict(int)
    GetSPO = Config.parse_triple_function(name)
    for ii, zz in tqdm(enumerate(ljqpy.LoadListg(fn))):
        try:
            s, p, o = GetSPO(zz)
        except:
            continue
        onums[o] += 1

    supports = {}
    lasts = ''
    striples = set()
    for ii, zz in tqdm(enumerate(ljqpy.LoadListg(fn))):
        try:
            s, p, o = GetSPO(zz)
        except:
            continue
        if s != lasts:
            for _, b, c in striples:
                for _, d, e in striples:
                    if (b, c) == (d, e): continue
                    x = 'x'
                    supports[(b, x, d, e)] = supports.get((b, x, d, e), 0) + 1
            striples = set()
        if onums[o] > onum:
            striples.add((s, p, o))
        lasts = s
        if len(supports) > 30000 and (ii % 1000000 == 0 or len(supports) > 4000000):
            print('make rule list ...', ii, len(supports))
            rules = ljqpy.FreqDict2List(supports)
            rules = [x for x in rules if x[1] > 1][:20000]
            ljqpy.SaveCSV(rules, './rule/%s/rules_2_%s.txt' % (name, name))
            supports = {x: y for x, y in rules}
    print('make rule list ...', ii, len(supports))
    rules = ljqpy.FreqDict2List(supports)
    rules = [x for x in rules if x[1] > 1][:20000]
    ljqpy.SaveCSV(rules, './rule/%s/rules_2_%s.txt' % (name, name))



def Rules2_Conf(name, fn):
    # 规则
    rules = {eval(x): y for x, y in ljqpy.LoadCSV('./rule/%s/rules_2_%s.txt' % (name, name))}
    GetSPO = Config.parse_triple_function(name)
    eles = {(x[0], x[1]) for x in rules}
    striples = []
    supps = defaultdict(int)
    conds = defaultdict(int)
    cases = {}
    lasts = ''
    allcount = 0

    def make_rule_details():
        print('make rule details ...', ii)
        rdetails = []
        for c, supp in supps.items():  # supps.item: (p,o,p1,o1):24
            cc = (c[0], c[1])  # (p,x)
            cnum = conds[cc]
            conf = supp / cnum
            rdetails.append((c, supp, conf, cnum, cases[c], cases[cc]))
        rdetails.sort(key=lambda x: (-x[2], -x[1]))
        ljqpy.SaveCSV(rdetails, './rule/%s/rules_2_%s.txt' % (name, name))

    for ii, zz in tqdm(enumerate(ljqpy.LoadListg(fn))):
        try:
            s, p, o = GetSPO(zz)
        except:
            continue
        allcount += 1
        x = 'x'
        if s != lasts:
            for a, b, c in striples:
                if (b, x) not in eles: continue
                conds[(b, x)] += 1
                if len(cases.get((b, x), [])) < 20: cases.setdefault((b, x), []).append(a)
                for aa, d, e in striples:
                    assert aa == a
                    if (b, x, d, e) not in rules: continue
                    supps[(b, x, d, e)] += 1
                    if len(cases.get((b, x, d, e), [])) < 10:
                        cases.setdefault((b, x, d, e), []).append(a)
            striples = []

        if ii % 1000000 == 0: make_rule_details()
        striples.append((s, p, o))
        lasts = s
    make_rule_details()
    print('allcount:', allcount)


def Rules3_Supp(name, fn, onum=0):
    '''
        (s, p1, o1)(s, p2, o2) ->(s, p3, o3)
    '''
    onums = defaultdict(int)
    GetSPO = Config.parse_triple_function(name)
    for ii, zz in tqdm(enumerate(ljqpy.LoadListg(fn))):
        try:
            s, p, o = GetSPO(zz)
        except:
            continue
        onums[o] += 1

    supports = {}
    lasts = ''
    striples = set()
    for ii, zz in tqdm(enumerate(ljqpy.LoadListg(fn))):
        try:
            s, p, o = GetSPO(zz)
        except:
            continue

        if s != lasts or len(striples) >= 50:

            for _, b, c in striples:
                for _, d, e in striples:
                    if (b, c) == (d, e): continue
                    for _, f, g in striples:
                        if (b, c) == (f, g): continue
                        if (d, e) == (f, g): continue
                        supports[(b, c, d, e, f, g)] = supports.get((b, c, d, e, f, g), 0) + 1

            striples = set()
        if onums[o] > 100:
            striples.add((s, p, o))
        lasts = s
        if len(supports) > 30000 and (ii % 500000 == 0 or len(supports) > 4000000):
            print('make rule list ...', ii, len(supports))
            rules = ljqpy.FreqDict2List(supports)
            rules = [x for x in rules if x[1] > 1][:20000]
            ljqpy.SaveCSV(rules, './rule/%s/rules_3_%s.txt' % (name, name))
            supports = {x: y for x, y in rules}
    print('make rule list ...', ii, len(supports))
    rules = ljqpy.FreqDict2List(supports)
    rules = [x for x in rules if x[1] > 1][:20000]
    ljqpy.SaveCSV(rules, './rule/%s/rules_3_%s.txt' % (name, name))



def Rules3_Conf(name, fn):
    # 规则
    rules = {eval(x): y for x, y in ljqpy.LoadCSV('./rule/%s/rules_3_%s.txt' % (name, name))}
    GetSPO = Config.parse_triple_function(name)
    eles = {(x[0], x[1], x[2], x[3]) for x in rules}
    striples = []
    supps = defaultdict(int)
    conds = defaultdict(int)
    cases = {}
    lasts = ''
    allcount = 0

    def make_rule_details():
        print('make rule details ...', ii)
        rdetails = []
        for c, supp in supps.items():  # supps.item: (p,o,p1,o1,p2,o2):24
            cc = (c[0], c[1], c[2], c[3])  # (p,o,p1,o1)
            cnum = conds[cc]
            conf = supp / cnum
            rdetails.append((c, supp, conf, cnum, cases[c], cases[cc]))
        rdetails.sort(key=lambda x: (-x[2], -x[1]))
        ljqpy.SaveCSV(rdetails, './rule/%s/rules_3_%s.txt' % (name, name))

    for ii, zz in tqdm(enumerate(ljqpy.LoadListg(fn))):
        try:
            s, p, o = GetSPO(zz)
        except:
            continue
        allcount += 1
        if s != lasts:
            for a, b, c in striples:
                for aa, d, e in striples:
                    assert aa == a
                    if (b, c, d, e) not in eles: continue
                    conds[(b, c, d, e)] += 1
                    if len(cases.get((b, c, d, e), [])) < 20: cases.setdefault((b, c, d, e), []).append(a)
                    for aaa, f, g in striples:
                        assert aaa == a
                        if (b, c, d, e, f, g) not in rules: continue
                        supps[(b, c, d, e, f, g)] += 1
                        if len(cases.get((b, c, d, e, f, g), [])) < 10:
                            cases.setdefault((b, c, d, e, f, g), []).append(a)
            striples = []

        if ii % 100000 == 0: make_rule_details()
        striples.append((s, p, o))
        lasts = s
    make_rule_details()
    print('allcount:', allcount)


def get_entity_and_realtion(name, fn, check_data=False):
    entitys = set()
    relations = set()
    GetSPO = Config.parse_triple_function(name)
    for ii, zz in tqdm(enumerate(ljqpy.LoadListg(fn))):
        try:
            s, p, o = GetSPO(zz)
        except:
            continue
        if ii < 50 and check_data:
            print(s, '**', p, '**', o)
        entitys.add(s)
        relations.add(p)
    return len(entitys), len(relations)

def rule_mining(compress, name):
    if not os.path.exists('./temp/%s' % name):
        os.mkdir('./temp/%s' % name)
    if not os.path.exists('./rule/%s' % name):
        os.mkdir('./rule/%s' % name)
    # mining (s, p, o) ->(s, p1, o1)
    start = time.time()
    compress.mining_rule_1()
    # (s, p, x) ->(s, p1, o1)
    compress.mining_rule_2()
    # (s, p1, o1)(s, p2, o2) ->(s, p3, o3)
    compress.mining_rule_3()
    print('The rule mining process is done! Totoal mining time is：', time.time() - start)

def rerank_rule(name):
    for i in range(3):
        rule_type = i + 1
        all_rule = []

        rules_1 = ljqpy.LoadCSV('./rule/%s/rules_1_%s.txt' % (name, name))
        if os.path.exists('./rule/%s/rules_2_%s.txt' % (name, name)):
            rules_2 = ljqpy.LoadCSV('./rule/%s/rules_2_%s.txt' % (name, name))
        else:
            rules_2 = []
        rules_3 = ljqpy.LoadCSV('./rule/%s/rules_3_%s.txt' % (name, name))

        if rule_type == 1:
            all_rule.extend(rules_1)
            print('rule 1 count：', len(all_rule))
            all_rule.extend(rules_2)
            print('rule 2 count：', len(all_rule))

        if rule_type == 2:
            all_rule.extend(rules_3)
            print('rule 3 count：', len(all_rule))

        if rule_type == 3:
            all_rule.extend(rules_1)
            print('rule 1 count：', len(all_rule))
            all_rule.extend(rules_2)
            print('rule 2 count：', len(all_rule))
            all_rule.extend(rules_3)
            print('rule 3 count：', len(all_rule))

        all_rule.sort(key=lambda x: (int(x[1]) - (int(x[3]) - int(x[1]))), reverse=True)

        if rule_type == 1:
            ljqpy.SaveCSV(all_rule, './rule/%s/all_rules_rr_details_%s_1.txt' % (name, name))
        if rule_type == 2:
            ljqpy.SaveCSV(all_rule, './rule/%s/all_rules_rr_details_%s_2.txt' % (name, name))
        if rule_type == 3:
            ljqpy.SaveCSV(all_rule, './rule/%s/all_rules_rr_details_%s.txt' % (name, name))
    print('rule rerank done!')



def get_valid_rules(name, rule_type, rule_thre, rule_num):
    if rule_type == 3:
        valid_rules = ljqpy.LoadCSV('./rule/%s/all_rules_rr_details_%s.txt' % (name, name))
    if rule_type == 1:
        valid_rules = ljqpy.LoadCSV('./rule/%s/all_rules_rr_details_%s_1.txt' % (name, name))
    if rule_type == 2:
        valid_rules = ljqpy.LoadCSV('./rule/%s/all_rules_rr_details_%s_2.txt' % (name, name))
    vrules = []
    used = set()
    for rule in valid_rules:
        rr, supp, conf, cond, _, _ = rule
        supp, conf, cond = int(supp), float(conf), int(cond)
        if conf <= 0.5: continue
        if (supp * 2 - cond) < rule_thre: continue
        if rule_num:
            if len(vrules) > rule_num: break
        rr = eval(rr)
        if tuple(rr[-2:]) in used: continue
        used.add(tuple(rr[-2:]))
        if len(rr) == 4:  # rule 1,2
            cp, co, op, oo = rr
            if co == oo and cp == op: continue
            if co == 'x' and cp == op: continue
            vrules.append([cp, co, op, oo])
        else:  # rule 3
            cp, co, op, oo, dp, do = rr
            if (dp, do) in [(cp, co), (op, oo)]: continue
            vrules.append([cp, co, op, oo, dp, do])
    print("valid rules count:" + str(len(vrules)))
    return vrules


def write2file(data, file, files):

    ff = files.get(file)
    if not ff:
        files[file] = ff = open(file, 'w', encoding='utf-8')
    for item in data:
        ljqpy.WriteLine(ff, item)
    return files


def get_rules_exceptions(name):
    exceptions = set()
    used_rules = ljqpy.LoadCSV('./temp/%s/used_rules_details_%s.txt' % (name, name))
    used_rules.reverse()
    for line in ljqpy.LoadCSVg('./temp/%s/exceptions_details_%s.txt' % (name, name)):
        exceptions.add(tuple(line))
    # if ('./temp/exceptions2_details_%s.txt' % name) in os.listdir():
    if os.path.exists('./temp/%s/exceptions2_details_%s.txt' % (name, name)):
        for line in ljqpy.LoadCSVg('./temp/%s/exceptions2_details_%s.txt' % (name, name)):
            exceptions.add(tuple(line))

    return used_rules, exceptions


def compressKB(name, fn, vrules, usefastcheck, onumlim, files):

    """
        originalKB-》compressed KG
    """
    detailed_rules = {}
    GetSPO = Config.parse_triple_function(name)

    onums = defaultdict(int)
    for ii, zz in tqdm(enumerate(ljqpy.LoadListg(fn))):
        try:
            s, p, o = GetSPO(zz)
            onums[o] += 1
        except:
            continue

    fastcheck = {}
    if usefastcheck:
        for i, rule in enumerate(vrules):
            if rule[1] == 'x':
                fastcheck.setdefault(rule[0], set()).add(i)
            else:
                fastcheck.setdefault((rule[0], rule[1]), set()).add(i)

    lasts = ''
    striples = set()
    for ii, zz in tqdm(enumerate(ljqpy.LoadListg(fn))):
        try:
            s, p, o = GetSPO(zz)
        except:
            continue
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
                if (len(rule) == 4):
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
            files = write2file(striples, './temp/%s/remained_details_%s.txt' % (name, name), files)
            files = write2file(exceptions, './temp/%s/exceptions_details_%s.txt' % (name, name), files)
            striples.clear()
        striples.add((s, p, o))
        lasts = s
    files = write2file(striples, './temp/%s/remained_details_%s.txt' % (name, name), files)
    simplify_rule = []
    # print("The count of rules used in the compression：" + str(len(detailed_rules.keys())))
    for rule in vrules:
        if (detailed_rules.get(str(rule), 0) > 0):
            simplify_rule.append(rule)
    print("The count of rules used in the compression:" + str(len(simplify_rule)))
    if os.path.exists('./temp/%s/used_rules_details_%s.txt' % (name, name)):
        os.remove(r'./temp/%s/used_rules_details_%s.txt' % (name, name))
    files = write2file(simplify_rule, './temp/%s/used_rules_details_%s.txt' % (name, name), files)

    return files


def do_compress_KB(name, fn, rule_type, rule_thre, rule_num, usefastcheck, onumlim, files):

    if os.path.exists('./temp/%s/remained_details_%s.txt' % (name, name)):
        os.remove(r'./temp/%s/remained_details_%s.txt' % (name, name))
    if os.path.exists('./temp/%s/exceptions_details_%s.txt' % (name, name)):
        os.remove(r'./temp/%s/exceptions_details_%s.txt' % (name, name))
    if os.path.exists('./temp/%s/exceptions2_details_%s.txt' % (name, name)):
        os.remove(r'./temp/%s/exceptions2_details_%s.txt' % (name, name))
    if os.path.exists('./temp/%s/used_rules_details_%s.txt' % (name, name)):
        os.remove(r'./temp/%s/used_rules_details_%s.txt' % (name, name))

    valid_rules = get_valid_rules(name, rule_type, rule_thre, rule_num)
    files = compressKB(name, fn, valid_rules, usefastcheck, onumlim, files)
    print('the compression process is complete')
    return files



def decompressKB(name, usefastcheck, used_rules, exceptions, files):

    fndc = './temp/%s/remained_details_%s.txt' % (name, name)
    lasts = ''
    striples = set()
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
            head = lasts
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
                    if (rule[1] != 'x'):
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

            files = write2file(striples, './temp/%s/decompressKB_details_%s.txt' % (name, name), files)
            striples.clear()
        striples.add((s, p, o))
        lasts = s
    files = write2file(striples, './temp/%s/decompressKB_details_%s.txt' % (name, name), files)
    for file in files.values(): file.close()
    files = {}
    return files


def decompressKB2(name, usefastcheck, used_rules, exceptions, files):

    supp = set()
    fndc = './temp/%s/remained_details_%s.txt' % (name, name)
    lasts = ''
    striples = set()

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
            head = lasts
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
                        if (len(rule) == 4):
                            if (rule[1] != 'x'):
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

            files = write2file(striples, './temp/%s/decompressKB_details_%s.txt' % (name, name), files)
            striples.clear()
        striples.add((s, p, o))
        lasts = s
    files = write2file(striples, './temp/%s/decompressKB_details_%s.txt' % (name, name), files)

    for file in files.values(): file.close()
    # files = {}


def caculate_total_count(name, lenold):

    remain_count = 0
    exception_count = 0
    exception2_count = 0
    rule_count = 0
    for ii, zz in enumerate(ljqpy.LoadListg('./temp/%s/remained_details_%s.txt' % (name, name))):
        remain_count += 1
    for ii, zz in enumerate(ljqpy.LoadListg('./temp/%s/exceptions_details_%s.txt' % (name, name))):
        exception_count += 1
    for ii, zz in enumerate(ljqpy.LoadListg('./temp/%s/exceptions2_details_%s.txt' % (name, name))):
        exception2_count += 1
    for ii, zz in enumerate(ljqpy.LoadListg('./temp/%s/used_rules_details_%s.txt' % (name, name))):
        rule_count += 1
    count = remain_count + exception_count + exception2_count + rule_count
    print("remained triples after compression: " + str(remain_count))
    print("the number of exceptions in compression: " + str(exception_count))
    print("the number of exceptions in decompression: " + str(exception2_count))
    print("rule count：" + str(rule_count))
    print("The compression result of %s is : %d (%.4f)" % (name, count, count / lenold))


def do_decompress_KB(phase, name, fn, files, usefastcheck=True):

    if (phase == 1):
        print("*************** first decompression start! ***************")
        if os.path.exists('./temp/%s/decompressKB_details_%s.txt' % (name, name)):
            os.remove(r'./temp/%s/decompressKB_details_%s.txt' % (name, name))
        used_rules, exceptions = get_rules_exceptions(name)
        files = decompressKB(name=name, usefastcheck=usefastcheck, used_rules=used_rules,
                     exceptions=exceptions, files=files)
        old = set()
        new = set()
        GetSPO = Config.parse_triple_function(name)
        with open(fn, encoding="utf-8") as fin:
            for line in fin:
                try:
                    old.add(tuple(GetSPO(line.strip())))
                except:
                    print(line)
                    continue
        print("the number of triples in original_kg: ", len(old))
        lenold = len(old)
        lennew = 0
        for line in ljqpy.LoadCSVg('./temp/%s/decompressKB_details_%s.txt' % (name, name)):
            lennew += 1
            line = tuple(line)
            if line not in old: new.add(line)
        print("the number of triples after 1st decompression: " + str(lennew))
        assert lennew >= lenold
        print("exception count in decompression：" + str(len(new)))
        files = write2file(new, './temp/%s/exceptions2_details_%s.txt' % (name, name), files)
        print('*************** 1st decompression complete! ***************')
        for file in files.values(): file.close()
        return lenold

    if (phase == 2):
        print("*************** second decompression start! ***************")
        if os.path.exists('./temp/%s/decompressKB_details_%s.txt' % (name, name)):
            os.remove(r'./temp/%s/decompressKB_details_%s.txt' % (name, name))
        used_rules, exceptions = get_rules_exceptions(name)
        # print(len(used_rules), len(exceptions))
        decompressKB2(name=name, usefastcheck=usefastcheck, used_rules=used_rules,
                             exceptions=exceptions, files=files)
        count = 0
        for ii, zz in tqdm(enumerate(ljqpy.LoadListg('./temp/%s/decompressKB_details_%s.txt' % (name, name)))):
            count += 1
        print("the number of triples after 2nd decompression: " + str(count))

        print('*************** 2nd decompression complete! ***************')


