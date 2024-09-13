import itertools
import sys
from collections import defaultdict
import math
from pyspark import SparkContext
from operator import itemgetter

sc= SparkContext()

#define error class for computing MAE and RMSE
class ErrorClass(object):
	def __init__(self, data=None):
		self.truth, self.test = map(itemgetter(0), data), map(itemgetter(1), data) #load data

	def computeError(self, r1=None, r2=None):       
		maeSum = 0.0 
		rmseSum = 0.0
		for i in range(0, len(self.truth)):
			r1 = self.truth[i]
			r2 = float(self.test[i])
			maeSum += abs(r1 - r2)
			rmseSum += math.pow((r1 - r2),2) 
		return (round(abs(float(maeSum/len(self.test))), 6),round((math.sqrt(rmseSum/len(self.test))), 6)) #return (MAE,RMSE)

#find all combinations of movies according to users as key and preserve their ratings as value
def findComb(key, value):
	temp = []
	for val1,val2 in itertools.combinations(value,2):
		temp.append(((val1[0],val2[0]),(val1[1],val2[1])))
	return temp

#helper function to create desired tuple format
def makeTuple(elements):
	temp2 = []
	for a in elements:
		temp3 = []
		for b in a:
			temp3.append(b)
		temp2.append(tuple(temp3))
	return tuple(temp2)

#calculate the cosine similarity
def findSim(key, value):
	counter = 0
	sum1 = 0.0
	sum2 = 0.0
	sum3 = 0.0
	for val in value:
		sum1 += float(val[0]) * float(val[0])
		sum2 += float(val[0]) * float(val[1])		
		sum3 += float(val[1]) * float(val[1])
		counter += 1
	sim = cos(sum2,math.sqrt(sum1),math.sqrt(sum3))
	return (key,(sim,counter))

#helper function for finding cosine sim
def cos(top,r1,r2):
	bottom = r1 * r2
	return (top / (float(bottom))) if bottom else 0.0

#preserve only n nearest neigbhors
def findNeighbors(movie,sim,n):
	sim.sort(key=lambda x: x[1][0],reverse=True)
	return movie, sim[:n]

#convert into (first movie, (second movie,sim)) pair and return
def firstKey(movies,sim):
	(movie1,movie2) = movies
	return movie1,(movie2,sim)

#find top N recommendations based on threshold
def topN(movie,ratings,sims,threshold):
	sum1 = defaultdict(int)
	sum2 = defaultdict(int)
	for (mov,rating) in ratings:
		neighbors = sims.get(mov,None)
		if neighbors:
			for (neighbor,(similarity,n)) in neighbors:
 				if neighbor != mov:
					sum1[neighbor] += similarity * float(rating)
					sum2[neighbor] += float(similarity) 
	topList = [(sum/float(sum2[item]) if sum2[item] else 0.0,item) for item,sum in sum1.items()]
	topList.sort(reverse=True)
	return movie,topList[:threshold]	



data = sc.textFile(sys.argv[1]).map(lambda line: line.split('\t')).map(lambda fields: (fields[1],(fields[0],fields[3]))).groupByKey() #load preprocessed training file (user(movie,ratings))

job1Result = data.filter(lambda x : len(x[1]) > 1).map(lambda x : findComb(x[0],x[1])).flatMap(lambda x : x).groupByKey().mapValues(makeTuple) #compute all combinations of movies according to user, and create ((movie1,movie2)(rating1,rating2))

job2Result = job1Result.map(lambda x : findSim(x[0],x[1])) #find the cosine similarity of the movie pairs

predictResult = job2Result.map(lambda p: firstKey(p[0],p[1])).groupByKey().map(lambda p : (p[0], list(p[1]))).map(lambda p: findNeighbors(p[0],p[1],50)).collect() #find nearest neighbors 

test = sc.textFile(sys.argv[2]).map(lambda line: line.split('\t')).map(lambda fields: (fields[1],fields[0],fields[3])).collect() #bring in test file (user(movie,ratings))

test_list = defaultdict(list)

for row in test: #store test file into test list for indexing and iterating
	username=row[0]
	moviename=row[1]
	ratings=row[2]
	test_list[username] += [(moviename,ratings)]
		
simDict = {}
for (movie,sim) in predictResult: #store predictResult similarity into list for comparison with test list
	simDict[movie] = sim

sharedDict = sc.broadcast(simDict)

recommend = data.map(lambda x: topN(x[0],x[1],sharedDict.value,50)).collect() #find recommendations (top K threshold as 3rd parameter of topN function)

predictions = []
for (username,movierating) in recommend: #iterate through recommend RDD
	for (rating,movie) in movierating:
		for (testmovie,testrating) in test_list[username]:                
			if str(testmovie) == str(movie): #if movie namems match, append to prediction[] for MAE and RMSE calculation
				predictions.append((rating,testrating))


errorCalc = ErrorClass(predictions) #create new class called errorCalc, which will have the computeError() function
result = errorCalc.computeError()
print 'Weighted All MAE and RMSE:' #print result
print result

sc.stop()
