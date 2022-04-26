# 将之前的代码的主要功能全部整合到这个文件中
import functions
from config import *
import time
import os


class Compress:

    def __init__(self, name, file, onum, rule_thre):
        self.name = name
        self.file = file
        self.onum = onum
        self.rule_thre = rule_thre

    # mining rules like (s,p1,o1)->(s,p2,o2)
    def mining_rule_1(self):
        functions.Rules1_Supp(self.name, self.file, self.onum)
        functions.Rules1_Conf(self.name, self.file)
        print('The mining process of rule_1 done!')

    # mining rules like (s,p1,x)->(s,p2,o2)
    def mining_rule_2(self):
        functions.Rules2_Supp(self.name, self.file, self.onum)
        functions.Rules2_Conf(self.name, self.file)
        print('The mining process of rule_2 done!')

    # mining rules like (s,p1,o1),(s,p2,o2)->(s,p3,o3)
    def mining_rule_3(self):
        functions.Rules3_Supp(self.name, self.file, self.onum)
        functions.Rules3_Conf(self.name, self.file)
        print('The mining process of rule_3 done!')

    # compress and decompress the KB using the rules mining above
    def do_compress_decompress(self, rule_type, rule_num, usefastcheck):
        # 保存打开的文件对象，这样可以不用频繁的打开和关闭文件，提高代码运行速度
        files = {}
        # 第一次压缩
        start_time = time.time()
        print("***************压缩开始：")
        files = functions.do_compress_KB(name=self.name, fn=self.file, rule_type=rule_type,
                                         rule_thre=self.rule_thre, rule_num=rule_num, onumlim=self.onum,
                                         usefastcheck=usefastcheck, files=files)
        print("压缩结束时所花费的时间：", time.time() - start_time)
        for file in files.values(): file.close()
        files = {}
        lenold = functions.do_decompress_KB(phase=1, name=self.name, fn=self.file, files=files)
        print("从开始到第一次解压结束消耗的时间：", time.time() - start_time)
        functions.caculate_total_count(self.name, lenold=lenold)
        files = {}
        functions.do_decompress_KB(phase=2, name=self.name, fn=self.file, files=files)
        print("压缩解压消耗的总时间：", time.time() - start_time)
        print('The compress process is done!')


if __name__ == '__main__':

    # 需要处理的kg
    name = 'jdk'
    # 获取kg文件的存放位置
    kg_file = Config.data_path[name]
    # 三元组spo中o出现的次数阈值
    onum = Config.onum_rulethre[name][0]
    # 规则能够压缩的三元组的个数阈值
    rule_compress_thre = Config.onum_rulethre[name][1]
    # 根据配置文件新建规则挖掘对象
    compress = Compress(name=name, file=kg_file, onum=onum, rule_thre=rule_compress_thre)
    # 获取KG的统计数据
    entity_num, relation_num = functions.get_entity_and_realtion(name=name, fn=kg_file)
    print("知识库%s的头实体数量:" % name, entity_num, "关系数量:", relation_num)
    if not os.path.exists('./temp/%s' % name):
        os.mkdir('./temp/%s' % name)
    if not os.path.exists('./rule/%s' % name):
        os.mkdir('./rule/%s' % name)
    # # 挖掘第一类规则
    # start = time.time()
    # compress.mining_rule_1()
    # # 挖掘第二类规则
    # compress.mining_rule_2()
    # # 挖掘第三类规则
    # compress.mining_rule_3()
    # print('总的挖掘时间：', time.time() - start)
    # # 按可压缩的三元组个数从大到小排序
    # functions.rerank_rule(name)

    # 需要配置压缩解压的一些参数，主要用于实验分析
    rule_type = 3  # rule_type代表用哪一类规则，1代表body长度为1，2代表body长度为2，3代表所有规则
    rule_num = 0  # 选取可用规则的条数
    # 根据规则压缩KG，并且实现KG的解压缩
    compress.do_compress_decompress(rule_type=rule_type, rule_num=rule_num, usefastcheck=True)
