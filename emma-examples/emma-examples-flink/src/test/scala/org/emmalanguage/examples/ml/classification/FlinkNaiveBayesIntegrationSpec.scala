/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.emmalanguage
package examples.ml.classification

import api._
import api.Meta.Projections._
import examples.ml.model._

import breeze.linalg.{Vector => Vec}
import org.apache.flink.api.scala.ExecutionEnvironment

class FlinkNaiveBayesIntegrationSpec extends BaseNaiveBayesIntegrationSpec {

  def naiveBayes(input: String, lambda: Double, modelType: MType): Set[Model] =
    emma.onFlink {
      // read the input
      val data = for (line <- DataBag.readText(input)) yield {
        val record = line.split(",").map(_.toDouble)
        LVector(record.head, Vec(record.slice(1, record.length)))
      }
      // classification
      val result = NaiveBayes(lambda, modelType)(data)
      // fetch the result locally
      result.fetch().toSet[Model]
    }

  implicit lazy val flinkEnv = ExecutionEnvironment.getExecutionEnvironment
}
