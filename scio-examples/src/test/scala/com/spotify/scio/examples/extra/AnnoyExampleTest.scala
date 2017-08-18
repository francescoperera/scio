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

package com.spotify.scio.examples.extra

import com.spotify.scio.testing._
import com.sun.jna.Native

class AnnoyExampleTest extends PipelineSpec {

  "AnnoyExample" should "work" in {
    import annoy4s._
    val lib = Native.loadLibrary("annoy", classOf[AnnoyLibrary]).asInstanceOf[AnnoyLibrary]
    val r = new scala.util.Random(10)
    val data = (0 until 100).map(x => (x, Array.fill(40)(r.nextFloat())))
    val index = lib.createAngular(40)
    data.foreach(d => lib.addItem(index, d._1, d._2))
    lib.build(index, 10)
    val expected = data.map { d =>
      val result = Array(-1)
      val distances = Array(-1.0f)
      lib.getNnsByItem(index, d._1, 1,1, result, distances)
      s"${d._1},${result.head}"
    }
    JobTest[com.spotify.scio.examples.extra.AnnoyExample.type]
      .output(TextIO("nearest.txt"))(_ should containInAnyOrder (expected))
      .run()
  }

}
