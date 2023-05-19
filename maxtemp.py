from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Maxtemp")
sc = SparkContext(conf= conf)

def parseLine(line):
    fileds = line.split(',')
    stationID = fileds[0]
    entryType = fileds[2]
    temp = float(fileds[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID,entryType,temp)

lines = sc.textFile("file:/Users/gwanseobhani/SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0],x[2]))
maxTemps = stationTemps.reduceByKey(lambda x,y : max(x,y))
results = maxTemps.collect()



for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))


