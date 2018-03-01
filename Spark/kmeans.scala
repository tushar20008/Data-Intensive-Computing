def distance(p:Vector[Double], q:Vector[Double]) : Double = {
	var dist = math.sqrt(p.zip(q).map(pair =>
		math.pow((pair._1 - pair._2), 2)).reduce(_ + _));
	return dist
}

def distanceA(p:Array[Vector[Double]], q:Array[Vector[Double]]) : Double = {
	var dist = p.zip(q).map(pair => distance(pair._1, pair._2)).reduce(_ + _);
	dist = dist / p.length
	return dist
}

def closestpoint(q: Vector[Double], candidates: Array[Vector[Double]]): Vector[Double] = {
	var bestIndex = 0
	var closest = Double.PositiveInfinity
	for (i <- 0 until candidates.length) {
		val tempDist = distance(q, candidates(i))
		if (tempDist < closest) {
			closest = tempDist
			bestIndex = i
		}
	}
	return candidates(bestIndex)
}

def add_vec(v1: Vector[Double], v2: Vector[Double]): Vector[Double] = {
	var newVector = v1.zip(v2).map(pair => (pair._1 + pair._2));
	return newVector
}

def average(cluster: Iterable[Vector[Double]]): Vector[Double] = {
	val numVectors = cluster.size
	var out = Vector(0.0, 0.0)
	var it = cluster.toIterator
	while (it.hasNext) {
		out = add_vec(out, it.next())
	}
	var ret = out.map(x => (x / numVectors))
	return ret
}


var lines = sc.textFile("clustering_dataset.txt");



var data = lines.map(l => Vector.empty ++ l.split('\t').map(_.toDouble))

var k = 3

// data.foreach(println)


var centers = data.takeSample(false, k, 99)
var centersBefore = centers

//centers.foreach(println)

var minDist = 1e-6

var d = 1 + minDist

do {
	var closest = data.map(p => (closestpoint(p, centers), p))
	var pointsgroup = closest.groupByKey()
	var newCenters = pointsgroup.mapValues(ps => average(ps))
	var editedCenters = newCenters.values
	d = distanceA(centers, editedCenters.collect)
	centers = editedCenters.collect
	println("Distance = ")
	println(d)
} while (d > minDist)

println("Initial cluster centers")
centersBefore.foreach(println)

println("Centers after Kmeans cluster")
centers.foreach(println)



System.exit(0)
