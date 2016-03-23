/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import java.util.Random

import org.apache.commons.math3.distribution.PoissonDistribution
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

private[spark]
case class TsTree(
  start:Long
  ,end:Long
  ,partitions:Int
) {
  assert(start < end)
  assert(partitions <= (end - start))
  val timeWindow:Long = (Math.abs(end - start) / partitions.toLong).toLong

  private val root:TsTreeNode = {
    val tsroot = new TsTreeNode(this, null, start, end, new ArrayBuffer[TsTreeNode]())
    tsroot.childNodes ++= (for {
      childIndex <- 0 until partitions
      child: TsTreeNode = new TsTreeNode(this, tsroot, start + timeWindow * childIndex, start + timeWindow * (childIndex + 1), null)
    } yield child).toArray[TsTreeNode]
    tsroot
  }

  def insertValues(tsval:Array[Long]) = {

    tsval.foreach{ tk =>
      if (start <= tk && tk <= end) {
        val childIndex:Int = (((tk - start) / timeWindow) - 1).toInt
        root.childNodes(childIndex).insertValues(tk)
      }
    }
  }

  override def toString() : String =
    "Start [" + start + "] -> End [" + end + "] Partitions [" + partitions + "] timeWindow [" + timeWindow + "]" +
      " root.ChildNodes # (" + root.childNodes.size + ") "  + root.childNodes.map(_.toString)
}

private[spark]
case class TsTreeNode(
  tree:TsTree
  ,parentNode:TsTreeNode
  ,start:Long
  ,end:Long
  ,var childNodes:ArrayBuffer[TsTreeNode]
) {
  val tsKey:ArrayBuffer[Long] = new ArrayBuffer[Long]()
  def insertValues(aKey:Long) = {
    //todo : insertion sort
    tsKey.append(aKey)
  }

  override def toString() =
    "CN Start <" + start + "> End <" + end + "> tsKey (" + tsKey.toString() + ")"
}

private[spark]
class TsRDDPartition(val prev: Partition, val seed: Int) extends Partition with Serializable {
  override val index: Int = prev.index
}

private[spark]
class TsRDD[T: ClassTag](
    prev: RDD[T],
    withReplacement: Boolean,
    frac: Double,
    seed: Int)
  extends RDD[T](prev) {

  val rootTree = new TsTree(0, 100, 10)

  override def getPartitions: Array[Partition] = {
    val rg = new Random(seed)
    firstParent[T].partitions.map(x => new TsRDDPartition(x, rg.nextInt))
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    firstParent[T].preferredLocations(split.asInstanceOf[TsRDDPartition].prev)

  override def compute(splitIn: Partition, context: TaskContext): Iterator[T] = {
    val split = splitIn.asInstanceOf[TsRDDPartition]
    if (withReplacement) {
      // For large datasets, the expected number of occurrences of each element in a sample with
      // replacement is Poisson(frac). We use that to get a count for each element.
      val poisson = new PoissonDistribution(frac)
      poisson.reseedRandomGenerator(split.seed)

      firstParent[T].iterator(split.prev, context).flatMap { element =>
        val count = poisson.sample()
        if (count == 0) {
          Iterator.empty  // Avoid object allocation when we return 0 items, which is quite often
        } else {
          Iterator.fill(count)(element)
        }
      }
    } else { // Sampling without replacement
    val rand = new Random(split.seed)
      firstParent[T].iterator(split.prev, context).filter(x => (rand.nextDouble <= frac))
    }
  }
}
