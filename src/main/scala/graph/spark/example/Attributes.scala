package graph.spark.example

import scala.collection.immutable.Map

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

object Attributes
{
    def apply( label: String, properties: Map[String, Any] ): Attributes =
        new Attributes( label, properties )
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

class Attributes( val label: String, val properties: Map[String, Any] ) extends Serializable
{
    def property[T]( name: String ): T =
        properties.get( name ).get.asInstanceOf[T]
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
