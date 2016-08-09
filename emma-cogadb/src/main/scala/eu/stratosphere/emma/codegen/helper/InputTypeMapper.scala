package eu.stratosphere.emma.codegen.helper

import scala.reflect.runtime.universe._

object InputTypeMapper {

  def map(params: List[ValDef]) = {
    var resultMap = scala.collection.mutable.Map[String, (String, Type)]()
    for (param <- params) {
      param match {
        case q"$mods val $tname: $tpt = $expr" => {
          tpt match {
            case tpt@TypeTree() => {
              //map input name to class type
              resultMap += (tname.toString ->(TypeExtractor.extractNameFrom(tpt), tpt.tpe))
            }
          }
        }
      }
    }
    resultMap
  }
  
//  def extract(params: List[ValDef]) = {
//    for (param <- params) {
//      param match {
//        case q"$mods val $tname: $tpt = $expr" => {
//          tpt match {
//            case tpt@TypeTree() => {
//              //map input name to class type
//              resultMap += (tname.toString ->(TypeExtractor.extractNameFrom(tpt), tpt.tpe))
//            }
//          }
//        }
//      }
//    }
//  }
}
