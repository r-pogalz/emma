package eu.stratosphere.emma.codegen.cogadb

object CoGaCodeGenerator {

  def generateCode(tree: Stmt): String = {
    generate(tree)
  }

  private def generate(tree: Stmt): String = tree match {
    case StmtBlock(stmts) => generate(stmts)
    case IfThen(cond, thenStmt) =>
      s"if(${generate(cond)}){" +
        s"\n${generate(thenStmt)}}\n"
    case IfThenElse(cond, thenStmt, elseStmt) =>
      s"if(${generate(cond)}){" +
        s"\n${generate(thenStmt)}" +
        s"}else{" +
        s"\n${generate(elseStmt)}}\n"
    case Assignment(op, leftVal, rightVal) =>
      generate(leftVal) + op + generate(rightVal) + ";"
    case NoOp(expr) => s"${generate(expr)};\n"
  }

  private def generate(stmts: List[Stmt]): String = {
    if (stmts.isEmpty) {
      ""
    } else {
      val stmt = generate(stmts.head)
      stmt + generate(stmts.tail)
    }
  }

  private def generate(expr: Expr): String = expr match {
    case Const(name, tpe) => tpe match {
      case Type.STRING => s"${"\"" + name + "\""}"
      case Type.CHAR => s"'${name}'"
      case _ => name
    }
    case BinaryOp(op, left, right) =>
      op.format(generate(left), generate(right))
    case UnaryOp(op, expr) =>
      op.format(generate(expr))
    case VarDef(name, tpe) =>
      tpe.toString.format(name).trim
    case LocalVar(name) =>
      name
    case InputCol(name) =>
      s"#${name.toUpperCase}#"
    case OutputCol(name) =>
      s"#$name#"
  }
}
