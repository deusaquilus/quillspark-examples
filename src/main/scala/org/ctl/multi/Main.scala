package org.ctl.multi

import io.getquill._
import io.getquill.idiom.Idiom
import io.getquill.{NamingStrategy, PostgresDialect, SqlMirrorContext}
import io.getquill.context.Context

case class American(firstName:String, lastName:String, addressId:Int, age: Int, sane: Boolean)
case class Canadian(name:String, surname:String, residenceId:Int, age: Int, sane: Boolean)
case class Yeti(gruntingSound:String, roaringSound:String, caveId:Int, age: Int, sane: Boolean)

case class Humanoid(called:String, alsoCalled: String, age: Int, sane: Boolean)

trait PluggableContext[D <: Idiom, N <: NamingStrategy] extends Context[D, N] {
  val futureGrandparents = quote {
    (people: Query[Humanoid]) => people.filter(p => p.sane && p.age > 55)
  }
}

object Sql {
  val ctx = new SqlMirrorContext(PostgresDialect, Literal) with PluggableContext[PostgresDialect.type, Literal.type]
  import ctx._
  val futureGrandparentsQuery =
    run(futureGrandparents(query[Canadian].map(c =>
      Humanoid(c.name, c.surname, c.age, c.sane)
    ))).string
}

object Main {

  // TODO Example of where this is used in both a quill and spark context


}
