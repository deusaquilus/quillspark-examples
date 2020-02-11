import io.getquill._

def consolePrint(str: String) =
  str.split("\n").map("|" + _).mkString("\n")

val ctx = new SqlMirrorContext(SQLServerDialect, SnakeCase)
import ctx._

case class Person(id: Int, firstName: String, lastName: String, age: Int)
case class Vip(id: Int, firstName: String, lastName: String, somethingElse: String)
case class Address(personId: Int, street: String, zip: Int)

run(query[Vip]).string

// TODO Add arg
val filterJoes = quote {
  (q:Query[Person]) => q.filter(p => p.firstName == "Joe")
}

val joinAddresses = quote {
  (q: Query[Person]) =>
    for {
      p <- q
      a <- query[Address].leftJoin(a => a.personId == p.id)
    } yield (p, a)
}

val vipsAsPeople = quote {
  query[Vip].map(v => Person(v.id, v.firstName, v.lastName, 0))
}

// TODO Union
consolePrint(
  run(
    query[Person].filter(p => p.lastName == "Bloggs") //++ vipsAsPeople
  ).string(true)
)
