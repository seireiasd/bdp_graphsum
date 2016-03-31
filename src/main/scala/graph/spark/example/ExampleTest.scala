package graph.spark.example

import graph.spark.Attributes
import graph.spark.multi.{Mapper, Parallel, Pipe, Reducer}
import graph.spark.summarization.GraphSummarizerSimple
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
        val startTime = System.nanoTime()

        object CountAll
        {
            def apply[T](): Pipe[T, Long] =
            {
                Mapper[T, Long]( _ => 1L ) -> Reducer[Long]( ( c1: Long, c2: Long ) => c1 + c2 )
            }
        }

        object Sum
        {
            def apply[T](): Pipe[BigDecimal, BigDecimal] =
            {
                Reducer[BigDecimal]( ( c1: BigDecimal, c2: BigDecimal ) => c1 + c2 )
            }
        }

        object Max
        {
            def apply[T](): Pipe[BigDecimal, BigDecimal] =
            {
                Reducer[BigDecimal]( ( c1: BigDecimal, c2: BigDecimal ) => c1.max( c2 ) )
            }
        }

        object SelectAttribute
        {
            def apply[T]( name: String ): Pipe[Attributes, T] =
            {
                Mapper[Attributes, T]( x => x.property[T]( name ) )
            }
        }

        object ToKeyValue
        {
            def apply[T]( name: String ): Pipe[T, ( String, Any )] =
            {
                Mapper[T, ( String, Any )]( x => ( name, x ) )
            }
        }

        object CollectAttributes
        {
            def apply( label: String ): Pipe[( String, Any ), Attributes] =
            {
                Mapper[( String, Any ), Map[String, Any]]( x => Map( x ) ) ->
                Reducer[Map[String, Any]]( ( m1, m2 ) => m1 ++ m2 ) ->
                Mapper[Map[String, Any], Attributes]( x => Attributes( label, x ) )
            }
        }

        val labelSelector    = ( data: Attributes ) => data.label
        val vertexAggregator = ( label: String ) => label match
                               {
                                   case "SalesInvoice" => Parallel( Array( SelectAttribute[BigDecimal]( "revenue" ) -> Sum( ) -> ToKeyValue( "total revenue" ),
                                                                           CountAll[Attributes]() -> ToKeyValue( "count" ) ) ) ->
                                                          CollectAttributes( "SalesInvoice" )

                                   case "PurchInvoice" => Parallel( Array( SelectAttribute[BigDecimal]( "expense" ) -> Sum() -> ToKeyValue( "total expense" ),
                                                                           CountAll[Attributes]() -> ToKeyValue( "count" ) ) ) ->
                                                          CollectAttributes( "PurchInvoice" )

                                   case "Product"      => Parallel( Array( SelectAttribute[BigDecimal]( "price" ) -> Max() -> ToKeyValue( "max price" ),
                                                                           CountAll[Attributes]() -> ToKeyValue( "count" ) ) ) ->
                                                          CollectAttributes( "Product" )

                                   case _              => CountAll[Attributes]() -> ToKeyValue( "count" ) -> CollectAttributes( label )
                               }
        val edgeAggregator   = ( label: String ) => CountAll[Attributes]() -> ToKeyValue( "count" ) -> CollectAttributes( label )
        val summarizedGraph  = GraphSummarizerSimple( graph, labelSelector, vertexAggregator, labelSelector, edgeAggregator ).cache()
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
        val startTime = System.nanoTime()

        object CountAll
        {
            def apply[T](): Pipe[T, Long] =
            {
                Mapper[T, Long]( _ => 1L ) -> Reducer[Long]( ( c1: Long, c2: Long ) => c1 + c2 )
            }
        }

        object ToKeyValue
        {
            def apply[T]( name: String ): Pipe[T, ( String, Any )] =
            {
                Mapper[T, ( String, Any )]( x => ( name, x ) )
            }
        }

        object CollectAttributes
        {
            def apply( label: String ): Pipe[( String, Any ), Attributes] =
            {
                Mapper[( String, Any ), Map[String, Any]]( x => Map( x ) ) ->
                Reducer[Map[String, Any]]( ( m1, m2 ) => m1 ++ m2 ) ->
                Mapper[Map[String, Any], Attributes]( x => Attributes( label, x ) )
            }
        }

        object Cnt
        {
            private var cnt: Long = 0L

            def get: Long =
            {
                cnt += 1

                cnt
            }
        }

        val summarizedGraph = GraphSummarizerSimple( graph,
                                                     ( vd: Attributes ) => Cnt.get,
                                                     ( group: Long ) => CountAll[Attributes]() -> ToKeyValue( "count" ) -> CollectAttributes( group.toString ),
                                                     ( ed: Attributes ) => Cnt.get,
                                                     ( group: Long ) => CountAll[Attributes]() -> ToKeyValue( "count" ) -> CollectAttributes( group.toString ) )
/*
        val summarizedGraph = GraphSummarizerSimple( graph,
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
