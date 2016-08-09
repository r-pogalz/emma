package eu.stratosphere.emma.examples.math

import java.io.File

import eu.stratosphere.emma.examples.coga.EuclideanDist
import eu.stratosphere.emma.runtime.CoGaDB
import eu.stratosphere.emma.testutil._
import org.junit.experimental.categories.Category
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@Category(Array(classOf[ExampleTest]))
@RunWith(classOf[JUnitRunner])
class EuclideanDistTest extends FlatSpec with Matchers with BeforeAndAfter {
  // default parameters
  val dir = "/coga/euclideandistance"
  val path = tempPath(dir)

  before {
    new File(s"$path/output").mkdirs()
    materializeResource(s"$dir/points.csv")
    materializeResource(s"$dir/read_template.json")
    materializeResource(s"$dir/map_template.json")
    materializeResource(s"$dir/export_template.json")
  }

  after {
    deleteRecursive(new File(path))
  }

  "EuclideanDist" should "calculate dist" in withRuntime(new CoGaDB) { rt =>
    new EuclideanDist(path, s"$path/output", rt).run()
  }
}
