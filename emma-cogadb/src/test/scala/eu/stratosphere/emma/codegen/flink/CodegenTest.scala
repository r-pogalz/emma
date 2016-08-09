package eu.stratosphere
package emma.codegen.flink

import eu.stratosphere.emma.codegen.BaseCodegenTest
import eu.stratosphere.emma.runtime.CoGaDB

class CodegenTest extends BaseCodegenTest("flink") {

  override def runtimeUnderTest: CoGaDB =
    CoGaDB.testing()
}
