package org.ctl.complex

import java.util.ArrayList

import io.getquill.SqlMirrorContext

object Main3 {

  case class Address(street: String)
  case class Person(name: String, age: Int, addresses: List[Address])

  val people = List(
    Person("Joe", 123, List(Address("123"), Address("456")))
  )
  def main(args: Array[String]): Unit = {

    val output: ArrayList[Address] = new ArrayList();
    for (p <- people) {
      for (a <- p.addresses) {
        output.add(a)
      }
    }
    people.flatMap(p => p.addresses)
  }
}


object Main4 {

  case class Address(ownerFk: Int, street: String)
  case class Person(id: Int, name: String, age: Int)

  val people = List(
    Person(1, "Joe", 123),
    Person(1, "Jack", 123)
  )
  val addresses = List(
    Address(1, "123 St."),
    Address(2, "456 St."),
  )

  def main(args: Array[String]): Unit = {

    import io.getquill._
    val ctx = new SqlMirrorContext(PostgresDialect, Literal)
    import ctx._

    val output: ArrayList[Address] = new ArrayList();
    for (p <- people) {
      for (a <- addresses) {
        if (p.id == a.ownerFk) {
          output.add(a)
        }
      }
    }

    val output1 =
      people.flatMap(p => addresses.filter(a => p.id == a.ownerFk))

    val peopleQ = quote { query[Person] }
    val addressesQ = quote { query[Address] }

    val output2 = quote {
      for {
        p <- peopleQ
        a <- addressesQ.filter(a => p.id == a.ownerFk)
      } yield (p, a)
    }
    run(output2)

    // SELECT p.*, a.*
    // FROM People p, Addresses a
    // WHERE p.id = a.ownerFk

    val output3 = quote {
      for {
        p <- peopleQ
        a <- addressesQ.join(a => p.id == a.ownerFk)
      } yield (p, a)
    }
    run(output3)

    // SELECT p.*, a.*
    // FROM People p
    // JOIN Addresses a
    // ON p.id = a.ownerFk
  }
}