package graph.spark.parser

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

object SparkParser
{
    def parseJson[T: ClassTag]( input: RDD[String], schematizer: JsonNode => T ): RDD[T] =
    {
        val mapper = new ObjectMapper

        input.map( ( string: String ) => schematizer( mapper.readTree( string ) ) )
    }
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
