package graph.spark.summarization.property

import scala.collection.immutable.Map
import scala.collection.immutable.Seq

import org.apache.spark.graphx.Graph

import graph.spark.Attributes
import graph.spark.summarization.GraphSummarizerSimple

object PropertySummarizer
{
    def apply( graph:               Graph[Attributes, Attributes],
               vertexGroupSelector: Attributes => String,
               vertexAggregators:   Seq[( String, String, PropertyAggregator )],
               edgeGroupSelector:   Attributes => String,
               edgeAggregators:     Seq[( String, String, PropertyAggregator )] ):
        Graph[Attributes, Attributes] =
    {
        val vertexAggregator = new Aggregator( vertexGroupSelector, vertexAggregators )
        val edgeAggregator   = new Aggregator( edgeGroupSelector, edgeAggregators )

        GraphSummarizerSimple( graph,
                               vertexGroupSelector,
                               vertexAggregator.preAggregate,
                               vertexAggregator.aggregate,
                               vertexAggregator.postAggregate,
                               edgeGroupSelector,
                               edgeAggregator.preAggregate,
                               edgeAggregator.aggregate,
                               edgeAggregator.postAggregate )
    }

    @SerialVersionUID( 1L )
    private class Aggregator( groupSelector: Attributes => String, aggregators: Seq[( String, String, PropertyAggregator )] ) extends Serializable
    {
        def preAggregate( vd: Attributes ): Attributes =
        {
            val builder = Map.newBuilder[String, Any]

            // find all aggregation properties in vertex attributes and execute the respective preaggregation method

            for ( ( name, aggregateName, aggregator ) <- aggregators )
            {
                vd.properties.find( { case ( propName, propValue ) => { name == propName } } )
                             .foreach( { case ( propName, propValue ) => { builder += Tuple2( name, aggregator.preAggregate( propValue ) ) } } )
            }

            Attributes( groupSelector( vd ), builder.result() )
        }

        def aggregate( p1: Attributes, p2: Attributes ): Attributes =
        {
            val builder = Map.newBuilder[String, Any]

            // find all aggregation properties in vertex attributes and execute the respective aggregation method

            for ( ( name, aggregateName, aggregator ) <- aggregators )
            {
                val v1 = p1.properties.find( { case ( propName, propValue ) => { name == propName } } )
                val v2 = p2.properties.find( { case ( propName, propValue ) => { name == propName } } )

                if ( v1.isDefined && v1.isDefined )
                {
                    val ( propName1, propValue1 ) = v1.get
                    val ( propName2, propValue2 ) = v2.get

                    builder += Tuple2( name, aggregator.aggregate( propValue1, propValue2 ) )
                }
            }

            Attributes( p1.label, builder.result() )
        }

        def postAggregate( aggregate: Attributes ): Attributes =
        {
            val builder = Map.newBuilder[String, Any]

            // find all aggregation properties in vertex attributes, execute the respective postaggregation method and assign new property name

            for ( ( name, aggregateName, aggregator ) <- aggregators )
            {
                aggregate.properties.find( { case ( propName, propValue ) => { name == propName } } )
                                    .foreach( { case ( propName, propValue ) =>
                                                     { builder += Tuple2( aggregateName, aggregator.postAggregate( propValue ) ) } } )
            }

            Attributes( aggregate.label, builder.result() )
        }
    }
}
