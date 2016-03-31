package graph.spark.test

import graph.spark.example._
import graph.spark.multi.Parallel
import graph.spark.summarization.GraphSummarizer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.math.BigDecimal

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

class SimpleTest extends FunSuite with BeforeAndAfterAll with Matchers
{
    var context:    SparkContext                  = _
    var graph:      Graph[Attributes, Attributes] = _
    var labelCount: Long                          = 0L

    /*------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

    override def beforeAll()
    {
        Logger.getLogger( "org" ).setLevel( Level.WARN )
        Logger.getLogger( "akka" ).setLevel( Level.WARN )

        System.setProperty( "spark.ui.showConsoleProgress", "false" )

        context    = new SparkContext( new SparkConf().setAppName( "Spark Test" ).setMaster( "local[*]" ) )
        graph      = DataParser.parseGraph( "data/nodes.json", "data/edges.json", context ).cache()
        labelCount = graph.vertices.map( { case ( vertexId, data ) => data.label } ).distinct().count()
    }

    /*------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

    override def afterAll()
    {
        context.stop()
    }

    /*------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

    test( "Running simple summarizer" )
    {
        var vertexCount = 0L

        info( "it shouldn't throw exceptions" )

        noException should be thrownBy
        {
            val summarizedGraph = GraphSummarizer( graph,
                                                   ( vd: Attributes ) => vd.label,
                                                   ( vd: Attributes ) => ( vd.label, 1l ),
                                                   ( lc1: ( String, Long ), lc2: ( String, Long ) ) => ( lc1._1, lc1._2 + lc2._2 ),
                                                   ( lc: ( String, Long ) ) => lc,
                                                   ( ed: Attributes ) => ed.label,
                                                   ( ed: Attributes ) => ( ed.label, 1l ),
                                                   ( lc1: ( String, Long ), lc2: ( String, Long ) ) => ( lc1._1, lc1._2 + lc2._2 ),
                                                   ( lc: ( String, Long ) ) => lc )

            vertexCount = summarizedGraph.numVertices
        }

        info( "it should give " + labelCount + " summarized vertices" )

        assert( vertexCount === labelCount )
    }

    /*------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

    test( "Running multi-path summarizer" )
    {
        var vertexCount = 0L

        info( "it shouldn't throw exceptions" )

        noException should be thrownBy
        {
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

            vertexCount = summarizedGraph.numVertices
        }

        info( "it should give " + labelCount + " summarized vertices" )

        assert( vertexCount === labelCount )
    }
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
