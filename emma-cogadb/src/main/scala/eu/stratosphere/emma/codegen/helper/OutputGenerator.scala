package eu.stratosphere.emma.codegen.helper

import java.lang.System.currentTimeMillis

object OutputGenerator {

  def nextOutputIdentifier(): String = {
    s"COMPUTED_VAL_${currentTimeMillis}"
  }
}
