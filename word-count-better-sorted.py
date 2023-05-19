import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower()) #정규식, 전부 소문자로 바꾸고 .""등 없앰

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:/Users/gwanseobhani/SparkCourse/book.txt")
words = input.flatMap(normalizeWords) # flatmap으로 분리 일(문장)대다(단어) 

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y) #맵함수로 키값 -> reducebykey로 같은 키인 빈도수 셈
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey() # 키값의 위치를 바꾼후 정렬
results = wordCountsSorted.collect() 

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
