package graph.spark.summarization

import scala.math.min
import scala.reflect.ClassTag

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object GraphSummarizerSimple
{
    def apply[VD, ED, VKey: ClassTag, VDA: ClassTag, VDS: ClassTag, EKey: ClassTag, EDA: ClassTag, EDS: ClassTag]
             ( graph:                Graph[VD, ED],
               vertexGroupSelector:  VD => VKey,
               vertexPreAggregator:  VD => VDA,
               vertexAggregator:     ( VDA, VDA ) => VDA,
               vertexPostAggregator: VDA => VDS,
               edgeGroupSelector:    ED => EKey,
               edgePreAggregator:    ED => EDA,
               edgeAggregator:       ( EDA, EDA ) => EDA,
               edgePostAggregator:   EDA => EDS ):
        Graph[VDS, EDS] =
    {
        val keyIdRdd             = graph.vertices.map( { case ( vertexId, data ) => ( vertexGroupSelector( data ), vertexId ) } )
        val representativeKeyRdd = keyIdRdd.reduceByKey( min )
        val representativeMap    = keyIdRdd.join( representativeKeyRdd )
                                           .map( { case ( key, ( oldVertexId, newVertexId ) ) => ( oldVertexId, newVertexId ) } )

        val summarizedVertices = representativeMap.join( graph.vertices )
                                                  .map( { case ( oldVertexId, ( newVertexId, data ) ) =>
                                                               ( newVertexId, vertexPreAggregator( data ) ) } )
                                                  .reduceByKey( vertexAggregator )
                                                  .map( { case ( vertexId, data ) =>
                                                               ( vertexId, vertexPostAggregator( data ) ) } )

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

        Graph( summarizedVertices, summarizedEdges )
    }
}
