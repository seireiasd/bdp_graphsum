package graph.spark.parser

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper

object SparkParser
{
    def parseJson[T : ClassTag]( input: RDD[String], schematizer: JsonNode => T ): RDD[T] =
    {
        val mapper = new ObjectMapper

        input.map( ( string: String ) => schematizer( mapper.readTree( string ) ) )
    }
}
