package graph.spark.example

import java.math.BigDecimal

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph.graphToGraphOps

import graph.spark.summarization.GraphSummarizerSimple

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

        System.out.println( "vertices: " + graph.numVertices )
        System.out.println( "edges: " + graph.numEdges )
        System.out.println()

        System.out.println( "first Customer.name: " +
                            graph.vertices.filter( vertex => vertex._2.label.equals( "Customer" ) )
                                          .collect()( 0 )._2.property[String]( "name" ) )

        System.out.println( "first PurchOrderLine.quantity: " +
                            graph.edges.filter( edge => edge.attr.label.equals( "PurchOrderLine" ) )
                                       .collect()( 0 ).attr.property[BigDecimal]( "quantity" ) )

        val startTime = System.nanoTime()

        val summarizedGraph = GraphSummarizerSimple( graph,
                                                     ( vd: Attributes ) => vd.label,
                                                     ( vd: Attributes ) => ( vd.label, 1l ),
                                                     ( lc1: ( String, Long ), lc2: ( String, Long ) ) => ( lc1._1, lc1._2 + lc2._2 ),
                                                     ( lc: ( String, Long ) ) => lc,
                                                     ( ed: Attributes ) => ed.label,
                                                     ( ed: Attributes ) => ( ed.label, 1l ),
                                                     ( lc1: ( String, Long ), lc2: ( String, Long ) ) => ( lc1._1, lc1._2 + lc2._2 ),
                                                     ( lc: ( String, Long ) ) => lc )

        summarizedGraph.cache()

        System.out.println()
        System.out.println( "summarized vertices: " + summarizedGraph.numVertices )
        System.out.println( "summarized edges: " + summarizedGraph.numEdges )
        System.out.println()

        val endTime = System.nanoTime();

        System.out.println( "Elapsed milliseconds: " + ( endTime - startTime ) / 1000000 )
        System.out.println()

        val summarizedVertices = summarizedGraph.vertices.collect()
        val summarizedEdges    = summarizedGraph.edges.collect()

        System.out.println( "vertices:" )

        for ( vertex <- summarizedVertices )
        {
            System.out.println( "\t( " + vertex._1 + ",\t( " + vertex._2._1 + ", " + vertex._2._2 + " ) )" )
        }

        System.out.println()
        System.out.println( "edges:" )

        for ( edge <- summarizedEdges )
        {
            System.out.println( "\t( " + edge.srcId + ",\t" + edge.dstId + ",\t( " + edge.attr._1 + ", " + edge.attr._2 + " ) )" )
        }
    }
}
