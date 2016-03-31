package graph.spark.example

import graph.spark.multi.Parallel
import graph.spark.summarization.GraphSummarizer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}

import scala.math.BigDecimal

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

object ExampleTest
{
    def main( args: Array[String] )
    {
        Logger.getLogger( "org" ).setLevel( Level.WARN )
        Logger.getLogger( "akka" ).setLevel( Level.WARN )

        System.setProperty( "spark.ui.showConsoleProgress", "false" )

        val context   = new SparkContext( new SparkConf().setAppName( "Spark Test" ).setMaster( "local[*]" ) )
        val graph     = DataParser.parseGraph( "data/nodes.json", "data/edges.json", context, 4 ).cache()
        val formatter = java.text.NumberFormat.getIntegerInstance

        println()
        println( "vertices: " + formatter.format( graph.numVertices ) )
        println( "edges: " + formatter.format( graph.numEdges ) )
        println()

        test2( graph )

        context.stop()
    }

    /*------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

    def test1( graph: Graph[Attributes, Attributes] )
    {
        val startTime       = System.nanoTime()
        val summarizedGraph = GraphSummarizer( graph,
                                               ( vd: Attributes ) => vd.label,
                                               ( vd: Attributes ) => ( vd.label, 1l ),
                                               ( lc1: ( String, Long ), lc2: ( String, Long ) ) => ( lc1._1, lc1._2 + lc2._2 ),
                                               ( lc: ( String, Long ) ) => lc,
                                               ( ed: Attributes ) => ed.label,
                                               ( ed: Attributes ) => ( ed.label, 1l ),
                                               ( lc1: ( String, Long ), lc2: ( String, Long ) ) => ( lc1._1, lc1._2 + lc2._2 ),
                                               ( lc: ( String, Long ) ) => lc )

        summarizedGraph.cache()

        val formatter = java.text.NumberFormat.getIntegerInstance

        println( "summarized vertices: " + formatter.format( summarizedGraph.numVertices ) )
        println( "summarized edges: " + formatter.format( summarizedGraph.numEdges ) )
        println()

        val endTime = System.nanoTime()

        println( "Elapsed milliseconds: " + formatter.format( ( endTime - startTime ) / 1000000 ) )
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

    /*------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

    def test2( graph: Graph[Attributes, Attributes] )
    {
        val startTime        = System.nanoTime()
        val labelSelector    = ( data: Attributes ) => data.label
        val vertexAggregator = ( label: String ) => label match
                               {
                                   case "SalesInvoice" => Parallel( Array( SelectAttribute[BigDecimal]( "revenue" ) -> Sum() -> ToKeyValue( "total revenue" ),
                                                                           CountAll[Attributes]() -> ToKeyValue( "count" ) ) ) ->
                                                          CollectAttributes( "SalesInvoice" )

                                   case "PurchInvoice" => Parallel( Array( SelectAttribute[BigDecimal]( "expense" ) -> Sum() -> ToKeyValue( "total expense" ),
                                                                           CountAll[Attributes]() -> ToKeyValue( "count" ) ) ) ->
                                                          CollectAttributes( "PurchInvoice" )

                                   case "Product"      => Parallel( Array( SelectAttribute[BigDecimal]( "price" ) -> Max() -> ToKeyValue( "max price" ),
                                                                           SelectAttribute[String]( "category" ) -> Count[String]() -> ToKeyValue( "number of categories" ),
                                                                           CountAll[Attributes]() -> ToKeyValue( "count" ) ) ) ->
                                                          CollectAttributes( "Product" )

                                   case _              => CountAll[Attributes]() -> ToKeyValue( "count" ) -> CollectAttributes( label )
                               }
        val edgeAggregator   = ( label: String ) => CountAll[Attributes]() -> ToKeyValue( "count" ) -> CollectAttributes( label )
        val summarizedGraph  = GraphSummarizer( graph, labelSelector, vertexAggregator, labelSelector, edgeAggregator ).cache()
        val formatter        = java.text.NumberFormat.getIntegerInstance

        println( "summarized vertices: " + formatter.format( summarizedGraph.numVertices ) )
        println( "summarized edges: " + formatter.format( summarizedGraph.numEdges ) )
        println()

        val endTime = System.nanoTime()

        println( "Elapsed milliseconds: " + formatter.format( ( endTime - startTime ) / 1000000 ) )
        println()

        println( "vertices:" )

        val summarizedVertices = summarizedGraph.vertices.collect()
        val summarizedEdges    = summarizedGraph.edges.collect()

        for ( vertex <- summarizedVertices )
        {
            println( "\tVertex: " + vertex._1 )

            for ( prop <- vertex._2.properties )
            {
                println( "\t\t\t" + prop._1 + " = " + prop._2 )
            }
        }

        println()
        println( "edges:" )

        for ( edge <- summarizedEdges )
        {
            println( "\tEdge: " + edge.srcId + " -> " + edge.dstId )

            for ( prop <- edge.attr.properties )
            {
                println( "\t\t\t" + prop._1 + " = " + prop._2 )
            }
        }
    }

    /*------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

    def test3( graph: Graph[Attributes, Attributes] )
    {
        object Cnt
        {
            private var cnt: Long = 0L

            def get: Long =
            {
                cnt += 1

                cnt
            }
        }

        val startTime       = System.nanoTime()
        val summarizedGraph = GraphSummarizer( graph,
                                               ( vd: Attributes ) => Cnt.get,
                                               ( group: Long ) => CountAll[Attributes]() -> ToKeyValue( "count" ) -> CollectAttributes( group.toString ),
                                               ( ed: Attributes ) => Cnt.get,
                                               ( group: Long ) => CountAll[Attributes]() -> ToKeyValue( "count" ) -> CollectAttributes( group.toString ) )
/*
        val summarizedGraph = GraphSummarizer( graph,
                                               ( vd: Attributes ) => Cnt.get,
                                               ( vd: Attributes ) => ( vd.label, 1l ),
                                               ( lc1: ( String, Long ), lc2: ( String, Long ) ) => ( lc1._1, lc1._2 + lc2._2 ),
                                               ( lc: ( String, Long ) ) => lc,
                                               ( ed: Attributes ) => Cnt.get,
                                               ( ed: Attributes ) => ( ed.label, 1l ),
                                               ( lc1: ( String, Long ), lc2: ( String, Long ) ) => ( lc1._1, lc1._2 + lc2._2 ),
                                               ( lc: ( String, Long ) ) => lc )
*/
        summarizedGraph.cache()

        val formatter = java.text.NumberFormat.getIntegerInstance

        println( "summarized vertices: " + formatter.format( summarizedGraph.numVertices ) )
        println( "summarized edges: " + formatter.format( summarizedGraph.numEdges ) )
        println()

        val endTime = System.nanoTime()

        println( "Elapsed milliseconds: " + formatter.format( ( endTime - startTime ) / 1000000 ) )
        println()
    }
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
