[comment]: <> (下面是代码的整体说明和运行流程示意)

#   *run.py ------main file*
    the process of rule mining, rule rerank, and kg compression

#   *compress.py*
    mining_rule_1---mining rules like (s,p1,o1)->(s,p2,o2)
    mining_rule_2---mining rules like (s,p1,x)->(s,p2,o2)
    mining_rule_3---mining rules like (s,p1,o1),(s,p2,o2)->(s,p3,o3)
    do_compress_decompress---compress and decompress kg

#   *config.py*
    data_path---source filepath of kg
    onum_rulethre---paramter for rule mining
    parse_triple_function---data parsing function for different kg

#   *functions.py*
    Rules3_Supp，Rules3_Conf---rule mining
    rerank_rule---rule rerank
    do_compress_KB---compression
    do_decompress_KB---decompression

#   *running process*
    1、sort the kg by head entity, and put the sorted kg data into './dataset'
    2、add config info in config.py
    3、python run.py, and the temp output and the rule used in the compression process is in './temp' and './rule' respectively

> 
