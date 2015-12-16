package graph.spark.example

import java.math.BigDecimal

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph.graphToGraphOps

object Test
{
    def main( args: Array[String] )
    {
        Logger.getLogger( "org" ).setLevel( Level.WARN )
        Logger.getLogger( "akka" ).setLevel( Level.WARN )

        System.setProperty( "spark.ui.showConsoleProgress", "false" )

        val context = new SparkContext( new SparkConf().setAppName( "Spark Test" ).setMaster( "local[1]" ) )
        val graph   = DataParser.parseGraph( "data/nodes.json", "data/edges.json", context )

        graph.cache()

        System.out.println( "vertices: " + graph.numVertices );
        System.out.println( "edges: " + graph.numEdges );

        System.out.println( "first Customer.name: " +
                            graph.vertices.filter( vertex => vertex._2.label.equals( "Customer" ) )
                                          .collect()( 0 )._2.property[String]( "name" ) )

        System.out.println( "first PurchOrderLine.quantity: " +
                            graph.edges.filter( edge => edge.attr.label.equals( "PurchOrderLine" ) )
                                       .collect()( 0 ).attr.property[BigDecimal]( "quantity" ) )
    }
}
