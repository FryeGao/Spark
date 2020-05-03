"""
create on : 20200503
function: spark test word count/Top N
author:yuefengGao
"""
import sys
from pyspark import SparkContext,SparkConf  # 导入SparkContext,SparkConf

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage word count:",sys.stderr)
        sys.exit(-1)

    conf = SparkConf()
    sc = SparkContext(conf = conf)

    # 打印word count结果
    def printWCResult():
        counts = sc.textFile(sys.argv[1])\
        .flatmap(lambda x:x.split("\t"))\
        .map(lambda x:(x,1))\
        .reduceByKey(lambda a,b:a+b)

        output = counts.collect()
        for word,count in output:
            print("%s:%i"%(word,count))

    # 打印TopN 结果
    def printTopNResult():
        counts = sc.textFile(sys.argv[1])\
        .map(lambda x:x.split("\t"))\
        .map(lambda x:x[5])\
        .reduceByKey(lambda a,b:a+b)\
        .map(lambda x:(x[1],x[0])).sortByKey(False)\
        .map(lambda x:(x[1],x[0])).take(5)

    # 将word count结果写入文件
    def saveFile():
        sc.textFile(sys.argv[1])\
        .flatMap(lambda x:x.split("\t"))\
        .map(lambda x:(x,1))\
        .reduceByKey(lambda a,b:a+b)\
        .saveAsTextFile(sys.argv[2])

    saveFile()

    sc.stop()



