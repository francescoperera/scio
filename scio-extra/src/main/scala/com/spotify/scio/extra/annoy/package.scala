/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.spotify.scio.extra

import java.util.UUID

import com.spotify.scio.ScioContext
import com.spotify.scio.values.{SCollection, SideInput}
import org.apache.beam.sdk.transforms.{DoFn, View}
import org.apache.beam.sdk.values.PCollectionView
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


/**
 * Main package for Annoy side input APIs. Import all.
 *
 * {{{
 *   import com.spotify.scio.extra.annoy._
 * }}}
 *
 * Two metrics are available, Angular and Euclidean.
 *
 * To save an `SCollection[(Int, Array[Float])]` to an Annoy file:
 *
 * {{{
 *   val s = sc.parallelize(Seq( 1-> Array(1.2f, 3.4f), 2 -> Array(2.2f, 1.2f)))
 * }}}
 *
 * // Save to a temporary location:
 * {{{
 *   val s1: SCollection[AnnoyUri] = s.asAnnoy(Angular, 40, 10)
 * }}}
 *
 * // Save to a specific location:
 * {{{
 *   val s1: SCollection[AnnoyUri] = s.asAnnoy(Angular, 40, 10, "gs://<bucket>/<path>")
 * }}}
 *
 * `SCollection[AnnoyUri]` can be converted into a side input:
 * {{{
 *   val s = sc.parallelize(Seq( 1-> Array(1.2f, 3.4f), 2 -> Array(2.2f, 1.2f)))
 *   val side: SideInput[AnnoyReader] = s.asAnnoySideInput(metric, dimension, numTrees)
 * }}}
 *
 * There's syntactic sugar for saving an SCollection and converting it to a side input:
 * {{{
 *   val s = sc.parallelize(Seq( 1-> Array(1.2f, 3.4f), 2 -> Array(2.2f, 1.2f)))
 *   .asAnnoySideInput(metric, dimension, numTrees)
 * }}}
 *
 * An existing Annoy file can be converted to a side input directly:
 * {{{
 *   sc.annoySideInput(metric, dimension, numTrees, "gs://<bucket>/<path>")
 * }}}
 *
 * `AnnoyReader` provides nearest neighbor lookups by vector as well as item lookups
 * {{{
 *  val data = (0 until 1000).map(x => (x, Array.fill(40)(r.nextFloat())))
 *  val main: SCollection[(Int, Array[Float])] = sc.parallelize(data)
 *  val side: SideInput[AnnoyReader] = main.asAnnoySideInput(metric, dimension, numTrees)
 *
 *  main.withSideInput(side)
 *  .map { (x, s) =>
 *    val annoyReader = s(side)
 *
 *    x.foreach { i =>
 *    annoyReader.getNearest(
 *      // get vector by item id, allocating a new vector each time
 *      val v1 = annoyIndex.getItemVector(i)
 *
 *      // get vector by item id, copy vector into pre-allocated Array[Float]
 *      val v2:Array[Float] = Array.fill(dim)(-1.0f)
 *      annoyIndex.getItemVector(i, v2)
 *
 *      // get 10 nearest neighbors by vector
 *      val results:Array[Int] = annoyIndex.getNearest(v2,10)
 *    }
 *  }
 *
 * }}}
 */
package object annoy {

  sealed trait ScioAnnoyMetric
  case object Angular extends ScioAnnoyMetric
  case object Euclidean extends ScioAnnoyMetric

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
   *  Wraps annoy-java
   */
  class AnnoyReader(metric: ScioAnnoyMetric, dim: Int, nTrees: Int, path: String) {

    import com.spotify.annoy._

    val indexType = metric match {
      case Angular => IndexType.ANGULAR
      case Euclidean => IndexType.EUCLIDEAN
    }

    val index = new ANNIndex(dim, path, indexType)

    def getItemVector(i: Int): Array[Float] = index.getItemVector(i)
    def getItemVector(i: Int, v: Array[Float]): Unit = index.getItemVector(i, v)
    def getNearest(v: Array[Float], maxNumResults: Int): Array[Int] = {
      index.getNearest(v, maxNumResults).asScala.map(Int.box(_).toInt).toArray[Int]
    }

  }

  /** Enhanced version of [[ScioContext]] with Annoy methods */
  implicit class AnnoyScioContext(val self: ScioContext) extends AnyVal {
    /**
      * Create a SideInput of `AnnoyIndex` from an [[AnnoyUri]] base path, to be used with
      * [[com.spotify.scio.values.SCollection.withSideInputs SCollection.withSideInputs]]
      */
    def annoySideInput(metric: ScioAnnoyMetric, dim: Int, nTrees: Int, path: String)
    : SideInput[AnnoyReader] = {
      val uri = AnnoyUri(path, self.options)
      val view = self.parallelize(Seq(uri)).applyInternal(View.asSingleton())
      new AnnoySideInput(view, metric, dim, nTrees)
    }
  }

  implicit class AnnoyPairSCollection(val self: SCollection[(Int, Array[Float])]) {
    /**
      * Write the key-value pairs of this SCollection as an Annoy file to a specific location.
      *
      * @return A singleton SCollection containing the [[AnnoyUri]] of the saved files
      */
    def asAnnoy(metric: ScioAnnoyMetric, dim: Int, nTrees: Int, path: String)
    : SCollection[AnnoyUri] = {
      val uri = AnnoyUri(path, self.context.options)
      require(!uri.exists, s"Annoy URI ${uri.path} already exists")

      self.transform { in =>
        in.groupBy(_ => ())
          .map { case(_, xs) =>
            logger.info(s"Saving as Annoy: $uri")
            val annoyIndex = new AnnoyWriter(metric, dim, nTrees)
            val it = xs.iterator
            while (it.hasNext) {
              val kv = it.next()
              annoyIndex.addItem(kv._1, kv._2)
            }
            uri.saveAndClose(annoyIndex)
            uri
          }
      }
    }

    /**
      * Write the key-value pairs of this SCollection as an Annoy file to a temporary location.
      *
      *   @return A Singleton SCollection containing the [[AnnoyUri]] of the saved files
      */
    def asAnnoy(metric: ScioAnnoyMetric, dim: Int, nTrees: Int)
    : SCollection[AnnoyUri] = {
      val uuid = UUID.randomUUID()
      val path = self.context.options.getTempLocation + s"/annoy-build-$uuid"
      this.asAnnoy(metric, dim, nTrees, path)
    }

    /**
     * Write the key-value pairs of this SCollection as an Annoy file to a temporary location
     * and load it as a SideInput[AnnoyReader]
     * @return SideInput[AnnoyReader]
     */
    def asAnnoySideInput(metric: ScioAnnoyMetric, dim: Int, nTrees: Int)
    : SideInput[AnnoyReader] = {
      self.asAnnoy(metric, dim, nTrees).asAnnoySideInput(metric, dim, nTrees)
    }
  }

  /**
    * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Annoy methods
    */
  implicit class AnnoySCollection(val self: SCollection[AnnoyUri]) extends AnyVal {
    def asAnnoySideInput(metric: ScioAnnoyMetric, dim: Int, nTrees: Int)
    : SideInput[AnnoyReader] = {
      val view = self.applyInternal(View.asSingleton())
      new AnnoySideInput(view, metric, dim, nTrees)
    }
  }

  private class AnnoySideInput(val view: PCollectionView[AnnoyUri],
                               metric: ScioAnnoyMetric,
                               dim: Int,
                               nTrees: Int)
    extends SideInput[AnnoyReader] {
    override def get[I,O](context: DoFn[I, O]#ProcessContext): AnnoyReader = {
      context.sideInput(view).getReader(metric, dim, nTrees)
    }
  }
}


