from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram") #local로 실행, 이름지정
sc = SparkContext(conf = conf) #sparkconf객체로 스파크 컨텍스트 객체를 만들고 sc로 정의

lines = sc.textFile("file:/Users/gwanseobhani/SparkCourse/ml-100k/u.data") #textfile로 RDD만듬
ratings = lines.map(lambda x: x.split()[2]) #맵함수로 새로운 RDD를 만듬
result = ratings.countByValue() # 특정값이 몇개인지 세는 함수 

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))# 결과정리
