package team.supernova

/**
 * Created by M06F525 on 10/05/2015.
 */
object testSpec {

  def testS {
    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    implicit val formats = DefaultFormats // Brings in default date formats etc.

    case class Child(name: String, age: Int, birthdate: Option[java.util.Date])
    case class Address(street: String, city: String)
    case class Person(name: String, address: Address, children: List[Child])
    val json = parse("""
         { "name": "joe",
           "address": {
             "street": "Bulevard",
             "city": "Helsinki"
           },
           "children": [
             {
               "name": "Mary",
               "age": 5,
               "birthdate": "2004-09-04T18:06:22Z"
             },
             {
               "name": "Mazy",
               "age": 3
             }
           ]
         }
                     """)

    println (json.extract[Person])


    val jsonList = parse("""
         [{ "name": "joe",
           "address": {
             "street": "Bulevard",
             "city": "Helsinki"
           },
           "children": [
             {
               "name": "Mary",
               "age": 5,
               "birthdate": "2004-09-04T18:06:22Z"
             },
             {
               "name": "Mazy",
               "age": 3
             }
           ]
         },
    { "name": "Gary",
      "address": {
        "street": "Bulevard",
        "city": "Helsinki"
      },
      "children": [
      {
        "name": "Mary",
        "age": 5,
        "birthdate": "2004-09-04T18:06:22Z"
      },
      {
        "name": "Mazy",
        "age": 3
      }
      ]
    }]
                         """)

    println (jsonList.extract[List[Person]])
  }



}
