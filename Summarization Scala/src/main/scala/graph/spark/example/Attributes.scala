package graph.spark.example

import scala.collection.immutable.Map

class Attributes( val label: String, val properties: Map[String, Any] ) extends Serializable
{
    def property[T]( name: String ): T = properties.get( name ).get.asInstanceOf[T]
}
