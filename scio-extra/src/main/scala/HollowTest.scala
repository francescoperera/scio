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

import java.util.UUID

import com.spotify.scio._
import com.spotify.scio.hollow.{HollowKVReader, HollowKVWriter, HollowStorage}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

object HollowTest {
  val path = "gs://scio-playground-us/hollow-100m"
}

object HollowWriteTest {
  def main(argz: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argz)

    val n = args.int("n")
    val m = args.long("m")
    val ids = args("ids")
    val out = HollowTest.path

    val kvs = sc.parallelize(Seq.fill(n)(m)).applyTransform(ParDo.of(new UUIDDoFn))
    kvs
      .groupBy(_ => ())
      .map { case (_, xs) =>
        val storage = HollowStorage.forFs(out)
        val writer = new HollowKVWriter(storage)
        writer.write(xs.iterator.map(kv => (kv._1.getBytes, kv._2.getBytes)))
        ()
      }
    kvs.keys.saveAsTextFile(ids)
    sc.close()
  }
}

object HollowReadTest {

  val reader = new HollowKVReader(HollowStorage.forFs(HollowTest.path))

  def main(argz: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argz)

    val ids = args("ids")
    val out = args("out")

    sc.textFile(ids)
      .map { id =>
        s"$id\t${Option(reader.getString(id)).getOrElse("null")}"
      }
      .saveAsTextFile(out)

    sc.close()
  }
}

class UUIDDoFn extends DoFn[Long, (String, String)] {
  @ProcessElement
  def processElement(c: DoFn[Long, (String, String)]#ProcessContext): Unit = {
    val n = c.element()
    var i = 0L
    while (i < n) {
      c.output((UUID.randomUUID().toString, UUID.randomUUID().toString))
      i += 1
    }
  }
}