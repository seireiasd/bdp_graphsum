package graph.spark.test

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.Graph.graphToGraphOps
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite
import org.scalatest.Matchers

import graph.spark.Attributes
import graph.spark.example.DataParser
import graph.spark.summarization.GraphSummarizerSimple
import graph.spark.summarization.property.PropertyCount
import graph.spark.summarization.property.PropertyMean
import graph.spark.summarization.property.PropertySum
import graph.spark.summarization.property.PropertySummarizer

class SimpleTest extends FunSuite with BeforeAndAfterAll with Matchers
{
    var context: SparkContext                  = _
    var graph:   Graph[Attributes, Attributes] = _

    override def beforeAll()
    {
        Logger.getLogger( "org" ).setLevel( Level.WARN )
        Logger.getLogger( "akka" ).setLevel( Level.WARN )

        System.setProperty( "spark.ui.showConsoleProgress", "false" )

        context = new SparkContext( new SparkConf().setAppName( "Spark Test" ).setMaster( "local[1]" ) )
        graph   = DataParser.parseGraph( "data/nodes.json", "data/edges.json", context )

        graph.cache()
    }

    override def afterAll()
    {
        context.stop()
    }

    test( "A simple summarizer shouldn't throw exceptions" )
    {
        noException should be thrownBy
        {
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

            summarizedGraph.numVertices
        }
    }

    test( "A property summarizer shouldn't throw exceptions either" )
    {
        noException should be thrownBy
        {
            val summarizedGraph = PropertySummarizer( graph,
                                                      ( vd: Attributes ) => vd.label,
                                                      List( ( "name", "number of names", PropertyCount() ) ),
                                                      ( ed: Attributes ) => ed.label,
                                                      List( ( "quantity", "mean quantity", PropertyMean[Int] ),
                                                            ( "salesPrice", "summed salesPrice", PropertySum[BigDecimal] ) ) )

            summarizedGraph.cache()

            summarizedGraph.numVertices
        }
    }
}
