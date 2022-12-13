package ca.uwaterloo.cs451.project

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

import scala.io.Source
import scala.math._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import util.control.Breaks._

class IslandConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output)
  val input = opt[String](descr = "graph data path", required = true)
  val output = opt[String](descr = "JSON plot data output path", required = true)
  val islands = opt[Int](descr = "number of islands", required = false, default = Some(4))
  verify()
}

object IslandVRP{
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new IslandConf(argv)
    val conf = new SparkConf()
        .setAppName("Running Islands")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    def readInstance(filePath: String) : (List[Int],List[(Int, Int)]) = {
        val bufferedSource = scala.io.Source.fromFile(filePath)
        val lines = (for (line <- bufferedSource.getLines()) yield line).toList
        bufferedSource.close

        val metadata = lines.take(3).map(_.toInt)
        val nodeCoords = lines.slice(3, metadata(2)+4)
        val nodes = nodeCoords.map(coordPair => {
            val coords = coordPair.split(" ")

            (coords(0).toInt, coords(1).toInt)
        })

        return (metadata, nodes)
    }

    val graphData = sc.broadcast(readInstance(args.input()))
    val survivorCount = sc.broadcast(1000)

    // force islands
    log.info("STARTING ISLANDS")
    val islands = sc.parallelize(List.range(0, args.islands()))
    val parallelIslands = islands.map(island => {
        // must define all functions for each island
        
        def checkPathList(individual: List[Int], loc: String) = {
            if(individual.sum != 210)
                println(s"!!!!! Little fucky wucky in $loc !!!!!!")
        }

        def getNode(nodeIdx: Int) : (Int, Int) = {
            return graphData.value._2(nodeIdx)
        }

        def fixPartition(p: List[Int], N: Int) : List[Int] = {
            if(p.sum == N)
                return p

            // winky face
            val ppSize = p.size
            val diff = signum(N - p.sum)
            var pMod = new ListBuffer[Int]()
            pMod ++= p

            while(pMod.sum != N) {
                val rand = new scala.util.Random
                val idx = rand.nextInt(ppSize)
                breakable {
                    if(diff < 0 && pMod(idx) == 0)
                        break
                    else 
                        pMod(idx) += diff
                }
            }

            return pMod.toList
        }

        val distance = (i1: (Int, Int), i2: (Int, Int)) => sqrt(pow((i1._1 - i2._1), 2) + pow((i1._2 - i2._2), 2))
        def computePathLength(depot: Int, path: List[Int]) : Double = {
            val N = path.length
            if(N == 0)
                return 0
            
            var totalPathDist = distance(getNode(depot), getNode(path(0)))
            for(nodeInPath <- 1 to N-1) {
                totalPathDist += distance(getNode(path(nodeInPath-1)), getNode(path(nodeInPath)))
            }
            totalPathDist += distance(getNode(path.last), getNode(depot))

            return totalPathDist
        }

        def fitness(depot: Int, individual: (List[Int],List[Int])) : Double = {
            val vehicles = individual._1
            val tour = individual._2
            // val N = tour.size

            var maxPathCost = -1.0
            var currPathLoc = 0
            for(v <- vehicles) {
                val vPath = tour.slice(currPathLoc, currPathLoc + v)
                val vPathCost = computePathLength(depot, vPath)
                currPathLoc += v

                if(maxPathCost < vPathCost)
                    maxPathCost = vPathCost
            }

            return -maxPathCost
        }

        def tournamentSelection(population: List[(List[Int],List[Int])], populationFitness: List[Double], eliteSize: Int, randomSelectionSize: Int, k: Int = 5) : List[(List[Int],List[Int])] = {
            val N = population.size
            val (sortedFitness, populationRanking) = populationFitness.zipWithIndex.sortBy(- _._1).unzip
            val elitePopulation = (for (idx <- 0 to eliteSize-1) yield population(populationRanking(idx))).toList

            var randomPopulation = new ListBuffer[(List[Int],List[Int])]()
            val rand = new scala.util.Random
            for(indiv <- 0 to randomSelectionSize-1) {
                val competitors = for(indiv <- 1 to k) yield rand.nextInt(N)
                val competitorFitness = for(c <- competitors) yield populationFitness(c)
                val winner = competitors(competitorFitness.indices.maxBy(competitorFitness))

                randomPopulation.append(population(winner))
            }

            return elitePopulation ++ randomPopulation.toList
        }

        // for vehicle crossover
        def uniformCrossover(v1: List[Int], v2: List[Int], N: Int) : List[Int] = {

            val vehicleCount = v1.size
            val flips = for(v <- 1 to vehicleCount) yield (random < 0.5)
            val child = (for(f <- 0 to flips.size-1) yield if(flips(f)) v1(f) else v2(f)).toList
            val legalChild = fixPartition(child, N)

            return legalChild
        }

        // for path crossover
        def orderCrossover(p1: List[Int], p2: List[Int]) : List[Int] = {
            val rand = new scala.util.Random
            val randPos = for(a <- 0 to 1) yield rand.nextInt(p1.size+1)
            val (pos1, pos2) = (randPos.min, randPos.max)

            val part1 = p1.slice(pos1,pos2)
            val inter = for(i <- p2) yield if(!part1.contains(i)) i else -1
            val part2 = inter.filter(_ != -1)

            return part2.slice(0, pos1+1) ++ part1 ++ part2.slice(pos1+1, part2.size)
            // return part1 ++ part2
        }

        // full chromosome crossover
        def crossover(i1: (List[Int],List[Int]), i2: (List[Int],List[Int])) : (List[Int],List[Int]) = {
            val N = i1._2.size
            val path = orderCrossover(i1._2, i2._2)

            // checkPathList(path, "crossover")

            return (uniformCrossover(i1._1, i2._1, N), path)
        }

        // let's get this bred
        def breed(selectedPopulation: List[(List[Int],List[Int])], eliteSize: Int, crossoverProbability: Double = 0.75) : List[(List[Int],List[Int])] = {
            val N = selectedPopulation.size

            val nepotism = (for(idx <- 0 to eliteSize-1) yield selectedPopulation(idx)).toList
            var offspring = new ListBuffer[(List[Int],List[Int])]()
            val rand = new scala.util.Random
            for(i <- 1 to N - eliteSize) {
                if(random < crossoverProbability) {
                    val p1 = selectedPopulation(rand.nextInt(N))
                    val p2 = selectedPopulation(rand.nextInt(N))
                    val child = crossover(p1, p2)
                    offspring.append(child)
                }
                else {
                    val child = selectedPopulation(rand.nextInt(N))
                    offspring.append(child)
                }
            }

            return nepotism ++ offspring.toList
        }

        def mutateVehicle(v: List[Int], N: Int, mutationProbability: Double) : List[Int] = {
            if(random < mutationProbability) {
                val rand = new scala.util.Random
                val pos = rand.nextInt(v.size)
                val vMutated = (for(i <- 0 to v.size-1) yield if(i != pos) v(i) else v.max)
                val vLegal = fixPartition(v, N)

                return vLegal
            }

            return v
        }

        def mutatePath(p: List[Int], mutationProbability: Double, mutationProbabilityAdjacent: Double) : List[Int] = {
            var pMut = new ListBuffer[Int]()
            pMut ++= p

            val rand = new scala.util.Random
            if(random < mutationProbability) {
                val pos1 = rand.nextInt(pMut.size)
                val pos2 = rand.nextInt(pMut.size)

                val pTemp = pMut(pos1)
                pMut(pos1) = pMut(pos2)
                pMut(pos2) = pTemp
            }

            if(random < mutationProbabilityAdjacent) {
                val pos2 = rand.nextInt(pMut.size)
                val pos1 = if(pos2 == 0) 1 else (pos2 - 1)

                val pTemp = pMut(pos1)
                pMut(pos1) = pMut(pos2)
                pMut(pos2) = pTemp
            }

            return pMut.toList
        }

        def mutate(population: List[(List[Int], List[Int])], eliteSize: Int, mutationProbability: Double = 0.01, mutationProbabilityAdjacent: Double = 0.01, uniformMutationProbability: Double = 0.01) : List[(List[Int], List[Int])] = {
            val elitePopulation = (for(i <- 0 to eliteSize-1) yield population(i)).toList

            var mutatedPopulation = new ListBuffer[(List[Int],List[Int])]()
            for(idx <- eliteSize to population.size-1) {
                val indiv = population(idx)
                val N = indiv._2.size
                val mutatedVehicles = mutateVehicle(indiv._1, N, uniformMutationProbability)
                val mutatedPath = mutatePath(indiv._2, mutationProbability, mutationProbabilityAdjacent)

                // checkPathList(mutatedPath, "mutation")
                
                mutatedPopulation.append((mutatedVehicles, mutatedPath))
            }

            return elitePopulation ++ mutatedPopulation.toList
        } 

        def runGA(depot: Int, populationSize: Int, eliteSize: Int, nVehicles: Int, crossoverProbability: Double = 0.75, mutationProbability: Double = 0.001, mutationProbabilityAdjacent: Double = 0.005, uniformMutationProbability: Double = 0.005, nIter: Int = 100, epsilon: Double = 0.00001) : ((List[Int], List[Int]), List[(List[Int], List[Int])], List[Double]) = {
            val N = graphData.value._1(2)
            val part = (for(i <- 1 to nVehicles) yield 0).toList
            val nodes = (for(i <- 1 to N) yield i).toList

            val populationList = (for(i <- 1 to populationSize) yield (fixPartition(part, N), scala.util.Random.shuffle(nodes)))
            var population = new ListBuffer[(List[Int],List[Int])]()
            population ++= populationList
            var fitnessValues = new ListBuffer[Double]()
            breakable {
                for(t <- 1 to nIter) {
                    val populationList = population.toList
                    val populationFitness = (for(idx <- 0 to populationSize-1) yield fitness(depot,populationList(idx))).toList
                    val selectedPopulation = tournamentSelection(populationList, populationFitness, eliteSize, populationSize-eliteSize, k=5)
                    val offspring = breed(selectedPopulation, eliteSize, crossoverProbability=crossoverProbability)
                    population.clear
                    population ++= mutate(offspring, eliteSize, mutationProbability=mutationProbability, mutationProbabilityAdjacent=mutationProbabilityAdjacent, uniformMutationProbability=uniformMutationProbability)
                    val currBestFit = populationFitness.max
                    if(fitnessValues.size > 10) {
                        val window = fitnessValues.slice(fitnessValues.size-5, fitnessValues.size).sum / 5
                        println(s"DIFF: ${currBestFit - window} !!!!")
                        if(currBestFit - window < epsilon) {
                            fitnessValues.append(currBestFit)
                            break
                        }
                    }
                    fitnessValues.append(currBestFit)
                    println(s"!!! Iteration $t, best fitness: ${fitnessValues.last} !!!")
                }
            }

            val populationListFinal = population.toList
            val populationFitness = (for(idx <- 0 to populationSize-1) yield fitness(depot,populationListFinal(idx))).toList
            val (sortedFitness, populationRanking) = populationFitness.zipWithIndex.sortBy(- _._1).unzip
            val best = populationListFinal(populationRanking(populationRanking(0)))
            fitnessValues.append(populationFitness.max)
            
            return (best, populationListFinal, fitnessValues.toList)
        }

        val (best, population, fitnessOverIter) = runGA(0, 10000, 100, 3, nIter=100, crossoverProbability=0.95, mutationProbability=0.005, mutationProbabilityAdjacent=0.05, uniformMutationProbability=0.05)
        

        population.take(survivorCount.value)
    }).persist

    var pops = ArrayBuffer[ArrayBuffer[(List[Int], List[Int])]]()
    parallelIslands.collect.foreach(survivors => {
        pops.append(survivors.to[ArrayBuffer])
    })
    if(args.islands() < 3) {
        for(idx <- 0 to pops.length - 1) {
            val rand = new scala.util.Random
            val islands = Random.shuffle((0 to pops.length - 1).filter(_ != idx))
            val island1 = islands(0)
            val island2 = islands(1)

            // must ensure that we migrate slices proportional to survivorCount
            val migrationCount = floor(survivorCount.value / 10).toInt

            pops(idx) = pops(idx).dropRight(migrationCount * 2)
            pops(idx) ++= pops(island1).take(migrationCount)
            pops(idx) ++= pops(island2).take(migrationCount)
        }
    }

    val newIslands = sc.parallelize(pops).map(survivors => {
        def checkPathList(individual: List[Int], loc: String) = {
            if(individual.sum != 210)
                println(s"!!!!! Little fucky wucky in $loc !!!!!!")
        }

        def getNode(nodeIdx: Int) : (Int, Int) = {
            return graphData.value._2(nodeIdx)
        }

        def fixPartition(p: List[Int], N: Int) : List[Int] = {
            if(p.sum == N)
                return p

            // winky face
            val ppSize = p.size
            val diff = signum(N - p.sum)
            var pMod = new ListBuffer[Int]()
            pMod ++= p

            while(pMod.sum != N) {
                val rand = new scala.util.Random
                val idx = rand.nextInt(ppSize)
                breakable {
                    if(diff < 0 && pMod(idx) == 0)
                        break
                    else 
                        pMod(idx) += diff
                }
            }

            return pMod.toList
        }

        val distance = (i1: (Int, Int), i2: (Int, Int)) => sqrt(pow((i1._1 - i2._1), 2) + pow((i1._2 - i2._2), 2))
        def computePathLength(depot: Int, path: List[Int]) : Double = {
            val N = path.length
            if(N == 0)
                return 0
            
            var totalPathDist = distance(getNode(depot), getNode(path(0)))
            for(nodeInPath <- 1 to N-1) {
                totalPathDist += distance(getNode(path(nodeInPath-1)), getNode(path(nodeInPath)))
            }
            totalPathDist += distance(getNode(path.last), getNode(depot))

            return totalPathDist
        }

        def fitness(depot: Int, individual: (List[Int],List[Int])) : Double = {
            val vehicles = individual._1
            val tour = individual._2
            // val N = tour.size

            var maxPathCost = -1.0
            var currPathLoc = 0
            for(v <- vehicles) {
                val vPath = tour.slice(currPathLoc, currPathLoc + v)
                val vPathCost = computePathLength(depot, vPath)
                currPathLoc += v

                if(maxPathCost < vPathCost)
                    maxPathCost = vPathCost
            }

            return -maxPathCost
        }

        def tournamentSelection(population: List[(List[Int],List[Int])], populationFitness: List[Double], eliteSize: Int, randomSelectionSize: Int, k: Int = 5) : List[(List[Int],List[Int])] = {
            val N = population.size
            val (sortedFitness, populationRanking) = populationFitness.zipWithIndex.sortBy(- _._1).unzip
            val elitePopulation = (for (idx <- 0 to eliteSize-1) yield population(populationRanking(idx))).toList

            var randomPopulation = new ListBuffer[(List[Int],List[Int])]()
            val rand = new scala.util.Random
            for(indiv <- 0 to randomSelectionSize-1) {
                val competitors = for(indiv <- 1 to k) yield rand.nextInt(N)
                val competitorFitness = for(c <- competitors) yield populationFitness(c)
                val winner = competitors(competitorFitness.indices.maxBy(competitorFitness))

                randomPopulation.append(population(winner))
            }

            return elitePopulation ++ randomPopulation.toList
        }

        // for vehicle crossover
        def uniformCrossover(v1: List[Int], v2: List[Int], N: Int) : List[Int] = {

            val vehicleCount = v1.size
            val flips = for(v <- 1 to vehicleCount) yield (random < 0.5)
            val child = (for(f <- 0 to flips.size-1) yield if(flips(f)) v1(f) else v2(f)).toList
            val legalChild = fixPartition(child, N)

            return legalChild
        }

        // for path crossover
        def orderCrossover(p1: List[Int], p2: List[Int]) : List[Int] = {
            val rand = new scala.util.Random
            val randPos = for(a <- 0 to 1) yield rand.nextInt(p1.size+1)
            val (pos1, pos2) = (randPos.min, randPos.max)

            val part1 = p1.slice(pos1,pos2)
            val inter = for(i <- p2) yield if(!part1.contains(i)) i else -1
            val part2 = inter.filter(_ != -1)

            return part2.slice(0, pos1+1) ++ part1 ++ part2.slice(pos1+1, part2.size)
            // return part1 ++ part2
        }

        // full chromosome crossover
        def crossover(i1: (List[Int],List[Int]), i2: (List[Int],List[Int])) : (List[Int],List[Int]) = {
            val N = i1._2.size
            val path = orderCrossover(i1._2, i2._2)

            // checkPathList(path, "crossover")

            return (uniformCrossover(i1._1, i2._1, N), path)
        }

        // let's get this bred
        def breed(N: Int, selectedPopulation: List[(List[Int],List[Int])], eliteSize: Int, crossoverProbability: Double = 0.75) : List[(List[Int],List[Int])] = {
            val nepotism = (for(idx <- 0 to eliteSize-1) yield selectedPopulation(idx)).toList
            var offspring = new ListBuffer[(List[Int],List[Int])]()
            val rand = new scala.util.Random
            for(i <- 1 to N - eliteSize) {
                if(random < crossoverProbability) {
                    val p1 = selectedPopulation(rand.nextInt(selectedPopulation.size))
                    val p2 = selectedPopulation(rand.nextInt(selectedPopulation.size))
                    val child = crossover(p1, p2)
                    offspring.append(child)
                }
                else {
                    val child = selectedPopulation(rand.nextInt(selectedPopulation.size))
                    offspring.append(child)
                }
            }

            return nepotism ++ offspring.toList
        }

        def mutateVehicle(v: List[Int], N: Int, mutationProbability: Double) : List[Int] = {
            if(random < mutationProbability) {
                val rand = new scala.util.Random
                val pos = rand.nextInt(v.size)
                val vMutated = (for(i <- 0 to v.size-1) yield if(i != pos) v(i) else v.max)
                val vLegal = fixPartition(v, N)

                return vLegal
            }

            return v
        }

        def mutatePath(p: List[Int], mutationProbability: Double, mutationProbabilityAdjacent: Double) : List[Int] = {
            var pMut = new ListBuffer[Int]()
            pMut ++= p

            val rand = new scala.util.Random
            if(random < mutationProbability) {
                val pos1 = rand.nextInt(pMut.size)
                val pos2 = rand.nextInt(pMut.size)

                val pTemp = pMut(pos1)
                pMut(pos1) = pMut(pos2)
                pMut(pos2) = pTemp
            }

            if(random < mutationProbabilityAdjacent) {
                val pos2 = rand.nextInt(pMut.size)
                val pos1 = if(pos2 == 0) 1 else (pos2 - 1)

                val pTemp = pMut(pos1)
                pMut(pos1) = pMut(pos2)
                pMut(pos2) = pTemp
            }

            return pMut.toList
        }

        def mutate(population: List[(List[Int], List[Int])], eliteSize: Int, mutationProbability: Double = 0.01, mutationProbabilityAdjacent: Double = 0.01, uniformMutationProbability: Double = 0.01) : List[(List[Int], List[Int])] = {
            val elitePopulation = (for(i <- 0 to eliteSize-1) yield population(i)).toList

            var mutatedPopulation = new ListBuffer[(List[Int],List[Int])]()
            for(idx <- eliteSize to population.size-1) {
                val indiv = population(idx)
                val N = indiv._2.size
                val mutatedVehicles = mutateVehicle(indiv._1, N, uniformMutationProbability)
                val mutatedPath = mutatePath(indiv._2, mutationProbability, mutationProbabilityAdjacent)

                // checkPathList(mutatedPath, "mutation")
                
                mutatedPopulation.append((mutatedVehicles, mutatedPath))
            }

            return elitePopulation ++ mutatedPopulation.toList
        } 

        def runMigratedGA(currPop: List[(List[Int],List[Int])], depot: Int, populationSize: Int, eliteSize: Int, nVehicles: Int, crossoverProbability: Double = 0.75, mutationProbability: Double = 0.001, mutationProbabilityAdjacent: Double = 0.005, uniformMutationProbability: Double = 0.005, nIter: Int = 100, epsilon: Double = 0.00001) : ((List[Int], List[Int]), List[(List[Int], List[Int])], List[Double]) = {
            var population = currPop.to[ListBuffer]
            var fitnessValues = new ListBuffer[Double]()
            breakable {
                for(t <- 1 to nIter) {
                    val populationList = population.toList
                    val populationFitness = (for(idx <- 0 to populationSize-1) yield fitness(depot,populationList(idx))).toList
                    val selectedPopulation = tournamentSelection(populationList, populationFitness, eliteSize, populationSize-eliteSize, k=5)
                    val offspring = breed(selectedPopulation.size, selectedPopulation, eliteSize, crossoverProbability=crossoverProbability)
                    population.clear
                    population ++= mutate(offspring, eliteSize, mutationProbability=mutationProbability, mutationProbabilityAdjacent=mutationProbabilityAdjacent, uniformMutationProbability=uniformMutationProbability)
                    val currBestFit = populationFitness.max
                    if(fitnessValues.size > 10) {
                        val window = fitnessValues.slice(fitnessValues.size-5, fitnessValues.size).sum / 5
                        println(s"DIFF: ${currBestFit - window} !!!!")
                        if(currBestFit - window < epsilon) {
                            fitnessValues.append(currBestFit)
                            break
                        }
                    }
                    fitnessValues.append(currBestFit)
                    println(s"!!! Iteration $t, best fitness: ${fitnessValues.last} !!!")
                }
            }
            val populationListFinal = population.toList
            val populationFitness = (for(idx <- 0 to populationSize-1) yield fitness(depot,populationListFinal(idx))).toList
            val (sortedFitness, populationRanking) = populationFitness.zipWithIndex.sortBy(- _._1).unzip
            val best = populationListFinal(populationRanking(populationRanking(0)))
            fitnessValues.append(populationFitness.max)
            
            return (best, populationListFinal, fitnessValues.toList)
        }

        val listOfSurvivors = survivors.toList
        val newPopulation = breed(survivorCount.value * 10, listOfSurvivors, survivorCount.value, crossoverProbability=0.95)
        val (best, populationList, fitnessValues) = runMigratedGA(newPopulation, 0, survivorCount.value * 10, (ceil(survivorCount.value / 10)).toInt, 3, nIter=50, crossoverProbability=0.95, mutationProbability=0.005, mutationProbabilityAdjacent=0.05, uniformMutationProbability=0.05)

        (fitnessValues.last, best)
    })

    newIslands.saveAsTextFile(args.output())
  }
}