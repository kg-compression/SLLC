import functions
from compress import Compress
from config import *



if __name__ == '__main__':

    # kg_name, should be the same as the one in the config.py
    name = 'fb15k'
    # kg filepath
    kg_file = Config.data_path[name]
    # threshold of onum and rule_compress_thre
    onum = Config.onum_rulethre[name][0]
    rule_compress_thre = Config.onum_rulethre[name][1]

    compress = Compress(name=name, file=kg_file, onum=onum, rule_thre=rule_compress_thre)
    # get the entity and relation count of kg
    entity_num, relation_num = functions.get_entity_and_realtion(name=name, fn=kg_file)
    print("The head entity count of %s is: " % name, entity_num, "relation count: ", relation_num)

    # rule mining
    functions.rule_mining(compress, name)
    # rerank the rule by ERN
    functions.rerank_rule(name)

    # rule_type means the type of ，1代表body长度为1，2代表body长度为2，3代表所有规则
    rule_type = 3
    # the number of rules that used for compres, and 0 means use all valid rules
    rule_num = 0
    # kg compression and decompression
    compress.do_compress_decompress(rule_type=rule_type, rule_num=rule_num, usefastcheck=True)
