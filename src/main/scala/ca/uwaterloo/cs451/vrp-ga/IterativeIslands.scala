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

import java.io._

object IterativeIslands{
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new IslandConf(argv)
    val conf = new SparkConf()
        .setAppName("Running Islands")
        // .set("spark.executor.instances", 4)
        .set("spark.shuffle.service.enabled", "true")
        .set("spark.dynamicAllocation.enabled", "true")
        .set("spark.executor.cores", "4")
        .set("spark.dynamicAllocation.minExecutors", "4")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    // this will hold all island fitness data
    var islandFitness = scala.collection.mutable.Map[Int,ArrayBuffer[Double]]()
    for (i <- 0 to args.islands() - 1) {
        islandFitness(i) = ArrayBuffer[Double]()
    }

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
    log.info("!!! Beginning Initial Epoch !!!")
    val islands = sc.parallelize(List.range(0, args.islands()))
    var parallelIslands = islands.map(island => {
        // must define all functions for each island
        def getNode(nodeIdx: Int) : (Int, Int) = {
            return graphData.value._2(nodeIdx)
        }

        def fixPartition(p: List[Int], N: Int) : List[Int] = {
            if(p.sum == N)
                return p

            val pSize = p.size
            val diff = signum(N - p.sum)
            var pMod = new ArrayBuffer[Int]()
            pMod ++= p

            while(pMod.sum != N) {
                val rand = new scala.util.Random
                val idx = rand.nextInt(pSize)
                breakable {
                    if (diff < 0 && pMod(idx) == 0)
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
            val pathArray = path.toArray
            if(N == 0)
                return 0
            
            var totalPathDist = distance(getNode(depot), getNode(pathArray(0)))
            for(nodeInPath <- 1 to N-1) {
                totalPathDist += distance(getNode(pathArray(nodeInPath-1)), getNode(pathArray(nodeInPath)))
            }
            totalPathDist += distance(getNode(pathArray.last), getNode(depot))

            return totalPathDist
        }

        def fitness(depot: Int, individual: (List[Int],List[Int])) : Double = {
            val vehicles = individual._1
            val tour = individual._2

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
            val populationFitnessArray = populationFitness.toArray
            val (sortedFitness, populationRanking) = populationFitnessArray.zipWithIndex.sortBy(- _._1).unzip
            val elitePopulation = (for (idx <- 0 to eliteSize-1) yield population(populationRanking(idx))).toList

            val rand = new scala.util.Random
            val randomPopulation = (0 to randomSelectionSize-1).map(indiv => {
                val competitors = for(indiv <- 1 to k) yield rand.nextInt(N)
                val competitorFitness = competitors.map(c => populationFitnessArray(c)) 
                val winner = competitors(competitorFitness.indices.maxBy(competitorFitness))

                population(winner)
            })

            return elitePopulation ++ randomPopulation.toList
        }

        // for vehicle crossover
        def uniformCrossover(v1: List[Int], v2: List[Int], N: Int) : List[Int] = {
            val vehicleCount = v1.size
            val flips = (1 to vehicleCount).map(_ => random < 0.5)
            val child = (0 to flips.size-1).map(f => if(flips(f)) v1(f) else v2(f)).toList
            val legalChild = fixPartition(child, N)

            return legalChild
        }

        // for path crossover
        def orderCrossover(p1: List[Int], p2: List[Int]) : List[Int] = {
            val rand = new scala.util.Random
            val randPos = for(a <- 0 to 1) yield rand.nextInt(p1.size+1)
            val (pos1, pos2) = (randPos.min, randPos.max)

            val part1 = p1.slice(pos1,pos2)
            val inter = p2.map(i => if (!part1.contains(i)) i else -1)
            val part2 = inter.filter(_ != -1)

            return part2.slice(0, pos1+1) ++ part1 ++ part2.slice(pos1+1, part2.size)
        }

        // full chromosome crossover
        def crossover(i1: (List[Int],List[Int]), i2: (List[Int],List[Int])) : (List[Int],List[Int]) = {
            val N = i1._2.size
            val path = orderCrossover(i1._2, i2._2)

            return (uniformCrossover(i1._1, i2._1, N), path)
        }

        def breed(selectedPopulation: List[(List[Int],List[Int])], eliteSize: Int, crossoverProbability: Double = 0.75) : List[(List[Int],List[Int])] = {
            val N = selectedPopulation.size
            val selectedPopulationArray = selectedPopulation.toArray

            val nepotism = (for(idx <- 0 to eliteSize-1) yield selectedPopulationArray(idx)).toList
            val rand = new scala.util.Random
            val offspring = (1 to N - eliteSize).map(i => {
                val child = if (random < crossoverProbability) {
                    val p1 = selectedPopulationArray(rand.nextInt(N))
                    val p2 = selectedPopulationArray(rand.nextInt(N))
                    crossover(p1, p2)
                }
                else {
                    selectedPopulationArray(rand.nextInt(N))
                }
                child
            })

            return nepotism ++ offspring.toList
        }

        def mutateVehicle(v: List[Int], N: Int, mutationProbability: Double) : List[Int] = {
            if(random < mutationProbability) {
                val rand = new scala.util.Random
                val pos = rand.nextInt(v.size)
                val vMax = v.max
                val vArray = v.toArray
                val vMutated = (0 to vArray.size-1).map(i => if(i != pos) vArray(i) else vMax)
                val vLegal = fixPartition(v, N)

                return vLegal
            }

            return v
        }

        def mutatePath(p: List[Int], mutationProbability: Double, mutationProbabilityAdjacent: Double) : List[Int] = {
            var pMut = new ArrayBuffer[Int]()
            pMut ++= p

            val rand = new scala.util.Random
            if (random < mutationProbability) {
                val pos1 = rand.nextInt(pMut.size)
                val pos2 = rand.nextInt(pMut.size)

                val pTemp = pMut(pos1)
                pMut(pos1) = pMut(pos2)
                pMut(pos2) = pTemp
            }

            if (random < mutationProbabilityAdjacent) {
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
            
            var mutatedPopulation = population.slice(eliteSize, population.size).map(indiv => {
                val N = indiv._2.size
                val mutatedVehicles = mutateVehicle(indiv._1, N, uniformMutationProbability)
                val mutatedPath = mutatePath(indiv._2, mutationProbability, mutationProbabilityAdjacent)
                
                (mutatedVehicles, mutatedPath)
            })

            return elitePopulation ++ mutatedPopulation.toList
        } 

        def runGA(depot: Int, populationSize: Int, eliteSize: Int, nVehicles: Int, crossoverProbability: Double = 0.75, mutationProbability: Double = 0.001, mutationProbabilityAdjacent: Double = 0.005, uniformMutationProbability: Double = 0.005, nIter: Int = 100, epsilon: Double = 0.00001, epsilonWindow: Int = 10) : ((List[Int], List[Int]), List[(List[Int], List[Int])], List[Double]) = {
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
                    if (fitnessValues.size > epsilonWindow+5) {
                        val window = fitnessValues.slice(fitnessValues.size-epsilonWindow, fitnessValues.size).sum / epsilonWindow
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
            val populationFitness = (for(idx <- 0 to populationSize-1) yield fitness(depot, populationListFinal(idx))).toList
            val (sortedFitness, populationRanking) = populationFitness.zipWithIndex.sortBy(- _._1).unzip
            val best = populationListFinal(populationRanking(populationRanking(0)))
            fitnessValues.append(populationFitness.max)
            
            return (best, populationListFinal, fitnessValues.toList)
        }

        val (best, population, fitnessOverIter) = runGA(0, 5000, 100, 3, nIter=50, crossoverProbability=0.95, mutationProbability=0.005, mutationProbabilityAdjacent=0.05, uniformMutationProbability=0.05)
        (island, population.take(survivorCount.value), fitnessOverIter)
    })

    // each iteration will trigger a migration after 50 generations on each island
    val numEpochs = 5 
    for(i <- 1 to numEpochs) {
        var pops = scala.collection.mutable.Map[Int, ArrayBuffer[(List[Int], List[Int])]]()
        for(j <- 0 to args.islands() - 1) pops(j) = ArrayBuffer[(List[Int], List[Int])]()
        parallelIslands.collect.foreach(data => {
            println(s"MOVING ISLAND ${data._1}")
            islandFitness(data._1) ++= data._3
            pops(data._1) = data._2.to[ArrayBuffer]
        })
        if (args.islands() >= 3) {
            for (idx <- 0 to args.islands()-1) {
                val rand = new scala.util.Random
                val islands = Random.shuffle((0 to args.islands() - 1).filter(_ != idx))
                val island1 = islands(0)
                val island2 = islands(1)

                // must ensure that we migrate slices proportional to survivorCount
                val migrationCount = floor(survivorCount.value / 10).toInt

                pops(idx) = pops(idx).dropRight(migrationCount * 2)
                pops(idx) ++= pops(island1).take(migrationCount)
                pops(idx) ++= pops(island2).take(migrationCount)
            }
        }

      if(i < numEpochs) {
        log.info(s"!!! Beginning Epoch $i !!!")
        parallelIslands = sc.parallelize(pops.toList).map(data => {
            val island = data._1
            val survivors = data._2
            // println(s"SIZE OF DATA FROM TUPLE: ${data._2.size}")
            // must define all functions for each island
            def getNode(nodeIdx: Int) : (Int, Int) = {
                return graphData.value._2(nodeIdx)
            }

            def fixPartition(p: List[Int], N: Int) : List[Int] = {
                if(p.sum == N)
                    return p

                val pSize = p.size
                val diff = signum(N - p.sum)
                var pMod = new ArrayBuffer[Int]()
                pMod ++= p

                while(pMod.sum != N) {
                    val rand = new scala.util.Random
                    val idx = rand.nextInt(pSize)
                    breakable {
                        if (diff < 0 && pMod(idx) == 0)
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
                val pathArray = path.toArray
                if(N == 0)
                    return 0
                
                var totalPathDist = distance(getNode(depot), getNode(pathArray(0)))
                for(nodeInPath <- 1 to N-1) {
                    totalPathDist += distance(getNode(pathArray(nodeInPath-1)), getNode(pathArray(nodeInPath)))
                }
                totalPathDist += distance(getNode(pathArray.last), getNode(depot))

                return totalPathDist
            }

            def fitness(depot: Int, individual: (List[Int],List[Int])) : Double = {
                val vehicles = individual._1
                val tour = individual._2

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
                val populationFitnessArray = populationFitness.toArray
                val (sortedFitness, populationRanking) = populationFitnessArray.zipWithIndex.sortBy(- _._1).unzip
                val elitePopulation = (for (idx <- 0 to eliteSize-1) yield population(populationRanking(idx))).toList

                val rand = new scala.util.Random
                val randomPopulation = (0 to randomSelectionSize-1).map(indiv => {
                    val competitors = for(indiv <- 1 to k) yield rand.nextInt(N)
                    val competitorFitness = competitors.map(c => populationFitnessArray(c)) 
                    val winner = competitors(competitorFitness.indices.maxBy(competitorFitness))

                    population(winner)
                })

                return elitePopulation ++ randomPopulation.toList
            }

            // for vehicle crossover
            def uniformCrossover(v1: List[Int], v2: List[Int], N: Int) : List[Int] = {
                val vehicleCount = v1.size
                val flips = (1 to vehicleCount).map(_ => random < 0.5)
                val child = (0 to flips.size-1).map(f => if(flips(f)) v1(f) else v2(f)).toList
                val legalChild = fixPartition(child, N)

                return legalChild
            }

            // for path crossover
            def orderCrossover(p1: List[Int], p2: List[Int]) : List[Int] = {
                val rand = new scala.util.Random
                val randPos = for(a <- 0 to 1) yield rand.nextInt(p1.size+1)
                val (pos1, pos2) = (randPos.min, randPos.max)

                val part1 = p1.slice(pos1,pos2)
                val inter = p2.map(i => if (!part1.contains(i)) i else -1)
                val part2 = inter.filter(_ != -1)

                return part2.slice(0, pos1+1) ++ part1 ++ part2.slice(pos1+1, part2.size)
            }

            // full chromosome crossover
            def crossover(i1: (List[Int],List[Int]), i2: (List[Int],List[Int])) : (List[Int],List[Int]) = {
                val N = i1._2.size
                val path = orderCrossover(i1._2, i2._2)

                return (uniformCrossover(i1._1, i2._1, N), path)
            }

            def breed(N: Int, selectedPopulation: List[(List[Int],List[Int])], eliteSize: Int, crossoverProbability: Double = 0.75) : List[(List[Int],List[Int])] = {
                val selectedPopulationArray = selectedPopulation.toArray
                val selectedPopulationArraySize = selectedPopulationArray.size
                val nepotism = (for(idx <- 0 to eliteSize-1) yield selectedPopulationArray(idx)).toList
                val rand = new scala.util.Random
                val offspring = (1 to N - eliteSize).map(i => {
                    val child = if (random < crossoverProbability) {
                        val p1 = selectedPopulationArray(rand.nextInt(selectedPopulationArraySize))
                        val p2 = selectedPopulationArray(rand.nextInt(selectedPopulationArraySize))
                        crossover(p1, p2)
                    }
                    else {
                        selectedPopulationArray(rand.nextInt(selectedPopulationArraySize))
                    }
                    child
                })
                return nepotism ++ offspring.toList
            }

            def mutateVehicle(v: List[Int], N: Int, mutationProbability: Double) : List[Int] = {
                if(random < mutationProbability) {
                    val rand = new scala.util.Random
                    val pos = rand.nextInt(v.size)
                    val vMax = v.max
                    val vArray = v.toArray
                    val vMutated = (0 to vArray.size-1).map(i => if(i != pos) vArray(i) else vMax)
                    val vLegal = fixPartition(v, N)

                    return vLegal
                }

                return v
            }

            def mutatePath(p: List[Int], mutationProbability: Double, mutationProbabilityAdjacent: Double) : List[Int] = {
                var pMut = new ArrayBuffer[Int]()
                pMut ++= p

                val rand = new scala.util.Random
                if (random < mutationProbability) {
                    val pos1 = rand.nextInt(pMut.size)
                    val pos2 = rand.nextInt(pMut.size)

                    val pTemp = pMut(pos1)
                    pMut(pos1) = pMut(pos2)
                    pMut(pos2) = pTemp
                }

                if (random < mutationProbabilityAdjacent) {
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
                
                var mutatedPopulation = population.slice(eliteSize, population.size).map(indiv => {
                    val N = indiv._2.size
                    val mutatedVehicles = mutateVehicle(indiv._1, N, uniformMutationProbability)
                    val mutatedPath = mutatePath(indiv._2, mutationProbability, mutationProbabilityAdjacent)
                    
                    (mutatedVehicles, mutatedPath)
                })

                return elitePopulation ++ mutatedPopulation.toList
            } 

            def runMigratedGA(currPop: List[(List[Int],List[Int])], depot: Int, populationSize: Int, eliteSize: Int, nVehicles: Int, crossoverProbability: Double = 0.75, mutationProbability: Double = 0.001, mutationProbabilityAdjacent: Double = 0.005, uniformMutationProbability: Double = 0.005, nIter: Int = 100, epsilon: Double = 0.00001, epsilonWindow: Int = 10) : ((List[Int], List[Int]), List[(List[Int], List[Int])], List[Double]) = {
                var population = currPop.to[ListBuffer]
                var fitnessValues = new ListBuffer[Double]()
                breakable {
                    for (t <- 1 to nIter) {
                        val populationList = population.toList
                        val populationFitness = (for(idx <- 0 to populationSize-1) yield fitness(depot,populationList(idx))).toList
                        val selectedPopulation = tournamentSelection(populationList, populationFitness, eliteSize, populationSize-eliteSize, k=5)
                        val offspring = breed(selectedPopulation.size, selectedPopulation, eliteSize, crossoverProbability=crossoverProbability)
                        population.clear
                        population ++= mutate(offspring, eliteSize, mutationProbability=mutationProbability, mutationProbabilityAdjacent=mutationProbabilityAdjacent, uniformMutationProbability=uniformMutationProbability)
                        val currBestFit = populationFitness.max
                        if (fitnessValues.size > epsilonWindow+5) {
                            val window = fitnessValues.slice(fitnessValues.size-epsilonWindow, fitnessValues.size).sum / epsilonWindow
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
            val newPopulation = breed(survivorCount.value * 5, listOfSurvivors, survivorCount.value, crossoverProbability=0.95)
            val (best, population, fitnessValues) = runMigratedGA(newPopulation, 0, survivorCount.value * 5, (ceil(survivorCount.value / 10)).toInt, 3, nIter=50, crossoverProbability=0.95, mutationProbability=0.005, mutationProbabilityAdjacent=0.05, uniformMutationProbability=0.05)

            (island, population.take(survivorCount.value), fitnessValues)
            // (fitnessValues.last, best)
        })
      }
      else {
        log.info("!!! Beginning Final Epoch !!!")

        val finalIslands = sc.parallelize(pops.toList).map(data => {
            val island = data._1
            val survivors = data._2
            // must define all functions for each island
            def getNode(nodeIdx: Int) : (Int, Int) = {
                return graphData.value._2(nodeIdx)
            }

            def fixPartition(p: List[Int], N: Int) : List[Int] = {
                if(p.sum == N)
                    return p

                val pSize = p.size
                val diff = signum(N - p.sum)
                var pMod = new ArrayBuffer[Int]()
                pMod ++= p

                while(pMod.sum != N) {
                    val rand = new scala.util.Random
                    val idx = rand.nextInt(pSize)
                    breakable {
                        if (diff < 0 && pMod(idx) == 0)
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
                val pathArray = path.toArray
                if(N == 0)
                    return 0
                
                var totalPathDist = distance(getNode(depot), getNode(pathArray(0)))
                for(nodeInPath <- 1 to N-1) {
                    totalPathDist += distance(getNode(pathArray(nodeInPath-1)), getNode(pathArray(nodeInPath)))
                }
                totalPathDist += distance(getNode(pathArray.last), getNode(depot))

                return totalPathDist
            }

            def fitness(depot: Int, individual: (List[Int],List[Int])) : Double = {
                val vehicles = individual._1
                val tour = individual._2

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
                val populationFitnessArray = populationFitness.toArray
                val (sortedFitness, populationRanking) = populationFitnessArray.zipWithIndex.sortBy(- _._1).unzip
                val elitePopulation = (for (idx <- 0 to eliteSize-1) yield population(populationRanking(idx))).toList

                val rand = new scala.util.Random
                val randomPopulation = (0 to randomSelectionSize-1).map(indiv => {
                    val competitors = for(indiv <- 1 to k) yield rand.nextInt(N)
                    val competitorFitness = competitors.map(c => populationFitnessArray(c)) 
                    val winner = competitors(competitorFitness.indices.maxBy(competitorFitness))

                    population(winner)
                })

                return elitePopulation ++ randomPopulation.toList
            }

            // for vehicle crossover
            def uniformCrossover(v1: List[Int], v2: List[Int], N: Int) : List[Int] = {
                val vehicleCount = v1.size
                val flips = (1 to vehicleCount).map(_ => random < 0.5)
                val child = (0 to flips.size-1).map(f => if(flips(f)) v1(f) else v2(f)).toList
                val legalChild = fixPartition(child, N)

                return legalChild
            }

            // for path crossover
            def orderCrossover(p1: List[Int], p2: List[Int]) : List[Int] = {
                val rand = new scala.util.Random
                val randPos = for(a <- 0 to 1) yield rand.nextInt(p1.size+1)
                val (pos1, pos2) = (randPos.min, randPos.max)

                val part1 = p1.slice(pos1,pos2)
                val inter = p2.map(i => if (!part1.contains(i)) i else -1)
                val part2 = inter.filter(_ != -1)

                return part2.slice(0, pos1+1) ++ part1 ++ part2.slice(pos1+1, part2.size)
            }

            // full chromosome crossover
            def crossover(i1: (List[Int],List[Int]), i2: (List[Int],List[Int])) : (List[Int],List[Int]) = {
                val N = i1._2.size
                val path = orderCrossover(i1._2, i2._2)

                return (uniformCrossover(i1._1, i2._1, N), path)
            }

            def breed(N: Int, selectedPopulation: List[(List[Int],List[Int])], eliteSize: Int, crossoverProbability: Double = 0.75) : List[(List[Int],List[Int])] = {
                val selectedPopulationArray = selectedPopulation.toArray
                val selectedPopulationArraySize = selectedPopulationArray.size
                val nepotism = (for(idx <- 0 to eliteSize-1) yield selectedPopulationArray(idx)).toList
                val rand = new scala.util.Random
                val offspring = (1 to N - eliteSize).map(i => {
                    val child = if (random < crossoverProbability) {
                        val p1 = selectedPopulationArray(rand.nextInt(selectedPopulationArraySize))
                        val p2 = selectedPopulationArray(rand.nextInt(selectedPopulationArraySize))
                        crossover(p1, p2)
                    }
                    else {
                        selectedPopulationArray(rand.nextInt(selectedPopulationArraySize))
                    }
                    child
                })
                return nepotism ++ offspring.toList
            }

            def mutateVehicle(v: List[Int], N: Int, mutationProbability: Double) : List[Int] = {
                if(random < mutationProbability) {
                    val rand = new scala.util.Random
                    val pos = rand.nextInt(v.size)
                    val vMax = v.max
                    val vArray = v.toArray
                    val vMutated = (0 to vArray.size-1).map(i => if(i != pos) vArray(i) else vMax)
                    val vLegal = fixPartition(v, N)

                    return vLegal
                }

                return v
            }

            def mutatePath(p: List[Int], mutationProbability: Double, mutationProbabilityAdjacent: Double) : List[Int] = {
                var pMut = new ArrayBuffer[Int]()
                pMut ++= p

                val rand = new scala.util.Random
                if (random < mutationProbability) {
                    val pos1 = rand.nextInt(pMut.size)
                    val pos2 = rand.nextInt(pMut.size)

                    val pTemp = pMut(pos1)
                    pMut(pos1) = pMut(pos2)
                    pMut(pos2) = pTemp
                }

                if (random < mutationProbabilityAdjacent) {
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
                
                var mutatedPopulation = population.slice(eliteSize, population.size).map(indiv => {
                    val N = indiv._2.size
                    val mutatedVehicles = mutateVehicle(indiv._1, N, uniformMutationProbability)
                    val mutatedPath = mutatePath(indiv._2, mutationProbability, mutationProbabilityAdjacent)
                    
                    (mutatedVehicles, mutatedPath)
                })

                return elitePopulation ++ mutatedPopulation.toList
            } 

            def runMigratedGA(currPop: List[(List[Int],List[Int])], depot: Int, populationSize: Int, eliteSize: Int, nVehicles: Int, crossoverProbability: Double = 0.75, mutationProbability: Double = 0.001, mutationProbabilityAdjacent: Double = 0.005, uniformMutationProbability: Double = 0.005, nIter: Int = 100, epsilon: Double = 0.00001, epsilonWindow: Int = 10) : ((List[Int], List[Int]), List[(List[Int], List[Int])], List[Double]) = {
                var population = currPop.to[ListBuffer]
                var fitnessValues = new ListBuffer[Double]()
                breakable {
                    for (t <- 1 to nIter) {
                        val populationList = population.toList
                        val populationFitness = (for(idx <- 0 to populationSize-1) yield fitness(depot,populationList(idx))).toList
                        val selectedPopulation = tournamentSelection(populationList, populationFitness, eliteSize, populationSize-eliteSize, k=5)
                        val offspring = breed(selectedPopulation.size, selectedPopulation, eliteSize, crossoverProbability=crossoverProbability)
                        population.clear
                        population ++= mutate(offspring, eliteSize, mutationProbability=mutationProbability, mutationProbabilityAdjacent=mutationProbabilityAdjacent, uniformMutationProbability=uniformMutationProbability)
                        val currBestFit = populationFitness.max
                        if (fitnessValues.size > epsilonWindow+5) {
                            val window = fitnessValues.slice(fitnessValues.size-epsilonWindow, fitnessValues.size).sum / epsilonWindow
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
            val newPopulation = breed(survivorCount.value * 5, listOfSurvivors, survivorCount.value, crossoverProbability=0.95)
            val (best, population, fitnessValues) = runMigratedGA(newPopulation, 0, survivorCount.value * 5, (ceil(survivorCount.value / 10)).toInt, 3, nIter=500, crossoverProbability=0.95, mutationProbability=0.005, mutationProbabilityAdjacent=0.05, uniformMutationProbability=0.05)

            (island, best, fitnessValues, fitnessValues.last)
        })
        var bestIsland = -1
        var bestFitness = 0.0
        var bestAns = (List[Int](), List[Int]())
        for(data <- finalIslands.collect) {
            islandFitness(data._1) ++= data._3
            if(bestIsland == -1 || data._4 > bestFitness) {
                bestIsland = data._1
                bestFitness = data._4
                bestAns = data._2
            }
        }
        val test = sc.parallelize(List[Any](bestAns, islandFitness.toList))
        test.saveAsTextFile(args.output())
        val pw = new PrintWriter(new File(s"${args.output()}/forPlot.txt" ))
        pw.write(bestAns.toString + "\n")
        for(i <- islandFitness) {
            pw.write(i._1 + ", " + i._2.toString + "\n")
        }
        pw.close
        // finalIslands.saveAsTextFile(args.output())
      }
      }
    }

}
