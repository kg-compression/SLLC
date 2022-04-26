[comment]: <> (下面是代码的整体说明和运行流程示意)

#   *compress.py ------代码运行主文件*
    mining_rule_1---挖掘第一类规则，如(s,p1,o1)->(s,p2,o2)
    mining_rule_2---挖掘第二类规则，如(s,p1,x)->(s,p2,o2)
    mining_rule_3---挖掘第三类规则，如(s,p1,o1),(s,p2,o2)->(s,p3,o3)
    do_compress_decompress---压缩解压流程，利用前面挖掘出来的3类规则，对KG中的triples进行压缩

#   *config.py ------配置文件*
    data_path---源数据文件存放的位置
    onum_rulethre---用于剪枝加速的配置参数，onum: 三元组spo中o出现的次数阈值 rule_thre 规则能够压缩的三元组的个数阈值
    parse_triple_function---解析源数据为s,p,o的函数，不同源数据文件的格式不一致，按需修改

#   *functions.py ------功能函数*
    Rules3_Supp，Rules3_Conf---规则挖掘
    rerank_rule---规则重排
    do_compress_KB---压缩
    do_decompress_KB---解压

#   *运行流程*
    1、将kg文件按照head entity排序后，将排序好之后的kg_file放到dataset文件夹下
    2、在config文件中添加该kg对应的配置信息
    3、compress.py中的main函数中修改运行参数，选择需要进行实验的kg，右键run,成功运行之后会在./rule和./temp路径下看到对应的输出文件

>
> 