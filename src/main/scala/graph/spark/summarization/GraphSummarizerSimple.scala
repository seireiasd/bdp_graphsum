package graph.spark.summarization

import scala.math.min
import scala.reflect.ClassTag

import scala.collection.immutable.Seq

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object GraphSummarizerSimple
{
    def apply[VD, ED, VDA: ClassTag, VDS: ClassTag, EDA: ClassTag, EDS: ClassTag]
             ( graph:                Graph[VD, ED],
               vertexGroupSelector:  VD => Any,
               vertexPreAggregator:  VD => VDA,
               vertexAggregator:     ( VDA, VDA ) => VDA,
               vertexPostAggregator: VDA => VDS,
               edgeGroupSelector:    ED => Any,
               edgePreAggregator:    ED => EDA,
               edgeAggregator:       ( EDA, EDA ) => EDA,
               edgePostAggregator:   EDA => EDS ):
        Graph[VDS, EDS] =
    {
        // select vertex groups

        val keyIdRdd = graph.vertices.map( { case ( vertexId, data ) => ( vertexGroupSelector( data ), vertexId ) } )

        // select vertex group representative ids ( min of all group vertex ids )

        val representativeKeyRdd = keyIdRdd.reduceByKey( min )

        // create a map ( old vertex id ) => ( new vertex id )

        val representativeMap = keyIdRdd.join( representativeKeyRdd )
                                        .map( { case ( key, ( oldVertexId, newVertexId ) ) => ( oldVertexId, newVertexId ) } )

        // assign group vertex ids and aggregate vertex groups

        val summarizedVertices = representativeMap.join( graph.vertices )
                                                  .map( { case ( oldVertexId, ( newVertexId, data ) ) =>
                                                               ( newVertexId, vertexPreAggregator( data ) ) } )
                                                  .reduceByKey( vertexAggregator )
                                                  .map( { case ( vertexId, data ) =>
                                                               ( vertexId, vertexPostAggregator( data ) ) } )

        // connect edges to their respective aggregated vertices, select edge groups and aggregate edge groups

        val summarizedEdges = graph.edges.map( ( edge: Edge[ED] ) => ( edge.srcId, ( edge.dstId, edge.attr ) ) )
                                         .join( representativeMap )
                                         .map( { case ( oldSourceId, ( ( oldTargetId, data ), newSourceId ) ) =>
                                                      ( oldTargetId, ( newSourceId, data ) ) } )
                                         .join( representativeMap )
                                         .map( { case ( oldTargetId, ( ( newSourceId, data ), newTargetId ) ) =>
                                                      ( ( newSourceId, newTargetId, edgeGroupSelector( data ) ), edgePreAggregator( data ) ) } )
                                         .reduceByKey( edgeAggregator )
                                         .map( { case ( ( sourceId, targetId, key ), data ) =>
                                                      Edge( sourceId, targetId, edgePostAggregator( data ) ) } )

        // return summarized graph

        Graph( summarizedVertices, summarizedEdges )
    }
}
