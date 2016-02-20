package graph.spark.example

import scala.math.BigDecimal

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps

import graph.spark.Attributes
import graph.spark.summarization.GraphSummarizerSimple
import graph.spark.summarization.property.PropertyCount
import graph.spark.summarization.property.PropertyMean
import graph.spark.summarization.property.PropertySum
import graph.spark.summarization.property.PropertySummarizer

object Test
{
    def main( args: Array[String] )
    {
        Logger.getLogger( "org" ).setLevel( Level.WARN )
        Logger.getLogger( "akka" ).setLevel( Level.WARN )

        System.setProperty( "spark.ui.showConsoleProgress", "false" )

        val context = new SparkContext( new SparkConf().setAppName( "Spark Test" ).setMaster( "local[*]" ) )
        val graph   = DataParser.parseGraph( "data/nodes.json", "data/edges.json", context ).cache()

        println()
        println( "vertices: " + graph.numVertices )
        println( "edges: " + graph.numEdges )
        println()

        test3( graph )
    }

    def test1( graph: Graph[Attributes, Attributes] )
    {
        println( "first Customer.name: " + graph.vertices.filter( vertex => vertex._2.label.equals( "Customer" ) )
                                                         .collect()( 0 )._2.property[String]( "name" ) )

        println( "first PurchOrderLine.quantity: " + graph.edges.filter( edge => edge.attr.label.equals( "PurchOrderLine" ) )
                                                                .collect()( 0 ).attr.property[BigDecimal]( "quantity" ) )
    }

    def test2( graph: Graph[Attributes, Attributes] )
    {
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

        println( "summarized vertices: " + summarizedGraph.numVertices )
        println( "summarized edges: " + summarizedGraph.numEdges )
        println()

        val endTime = System.nanoTime()

        println( "Elapsed milliseconds: " + ( endTime - startTime ) / 1000000 )
        println()

        val summarizedVertices = summarizedGraph.vertices.collect()
        val summarizedEdges    = summarizedGraph.edges.collect()

        println( "vertices:" )

        for ( vertex <- summarizedVertices )
        {
            println( "\t( " + vertex._1 + ",\t( " + vertex._2._1 + ", " + vertex._2._2 + " ) )" )
        }

        println()
        println( "edges:" )

        for ( edge <- summarizedEdges )
        {
            println( "\t( " + edge.srcId + ",\t" + edge.dstId + ",\t( " + edge.attr._1 + ", " + edge.attr._2 + " ) )" )
        }
    }

    def test3( graph: Graph[Attributes, Attributes] )
    {
        val startTime = System.nanoTime()

        val summarizedGraph = PropertySummarizer( graph,
                                                  ( vd: Attributes ) => vd.label,
                                                  List( ( "name", "number of names", PropertyCount() ) ),
                                                  ( ed: Attributes ) => ed.label,
                                                  List( ( "quantity", "mean quantity", PropertyMean[Int]() ),
                                                        ( "salesPrice", "summed salesPrice", PropertySum[BigDecimal]() ) ) )

        summarizedGraph.cache()

        println( "summarized vertices: " + summarizedGraph.numVertices )
        println( "summarized edges: " + summarizedGraph.numEdges )
        println()

        val endTime = System.nanoTime()

        println( "Elapsed milliseconds: " + ( endTime - startTime ) / 1000000 )
        println()

        val summarizedVertices = summarizedGraph.vertices.collect()
        val summarizedEdges    = summarizedGraph.edges.collect()

        println( "vertices:" )

        for ( vertex <- summarizedVertices )
        {
            if ( vertex._2.properties.exists( { case ( propName, propValue ) => { propName == "number of names" } } ) )
            {
                println( "\t( " + vertex._1 + ",\t( " + vertex._2.label + ", number of names: " + vertex._2.property[String]( "number of names" ) + " ) )" )
            }
            else
            {
                println( "\t( " + vertex._1 + ",\t( " + vertex._2.label + " ) )" )
            }
        }

        println()
        println( "edges:" )

        for ( edge <- summarizedEdges )
        {
            if ( edge.attr.label == "PurchOrderLine" )
            {
                println( "\t( " + edge.srcId + ",\t" + edge.dstId + ",\t( " +
                                  edge.attr.label + ", mean quantity: " + edge.attr.property[String]( "mean quantity" ) + " ) )" )
            }
            else if ( edge.attr.label == "SalesOrderLine" || edge.attr.label == "SalesQuotationLine" )
            {
                println( "\t( " + edge.srcId + ",\t" + edge.dstId + ",\t( " +
                                  edge.attr.label + ", mean quantity: " + edge.attr.property[String]( "mean quantity" ) +
                                                    ", summed salesPrice: " + edge.attr.property[String]( "summed salesPrice" ) + " ) )" )
            }
            else
            {
                println( "\t( " + edge.srcId + ",\t" + edge.dstId + ",\t( " + edge.attr.label + " ) ) )" )
            }
        }
    }
}
