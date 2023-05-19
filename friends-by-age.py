from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')  # ,마다 분리
    age = int(fields[2])  # 2번째 인덱스를 정수형으로 
    numFriends = int(fields[3]) # 3번째 인덱스를 정수형으로
    return (age, numFriends)

lines = sc.textFile("file:/Users/gwanseobhani/SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)  # 만든 함수를 map함수에 넣어 rdd생성
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) #mapValues 로 키는 그대로 두고 값만 변형 , reduceByKey 같은 키를 가진 값을 더함 
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1]) # [0]번 값을 [1]번값으로 나눔
results = averagesByAge.collect() # action
for result in results:
    print(result)
