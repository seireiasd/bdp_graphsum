package graph.spark.summarization

import scala.math.min
import scala.reflect.ClassTag

import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object GraphSummarizerSimple
{
    def apply[VD, ED, VKey: ClassTag, VDA: ClassTag, VDS: ClassTag, EKey: ClassTag, EDA: ClassTag, EDS: ClassTag]
    ( graph: Graph[VD, ED],
      vertexGroupSelector: VD => VKey,
      vertexPreAggregator: VD => VDA,
      vertexAggregator: ( VDA, VDA ) => VDA,
      vertexPostAggregator: VDA => VDS,
      edgeGroupSelector: ED => EKey,
      edgePreAggregator: ED => EDA,
      edgeAggregator: ( EDA, EDA ) => EDA,
      edgePostAggregator: EDA => EDS ): Graph[VDS, EDS] =
    {
/*
        val keyIdRdd             = graph.vertices.map( ( vertex: ( VertexId, VD ) ) => ( vertexGroupSelector( vertex._2 ), vertex._1 ) )
        val representativeKeyRdd = keyIdRdd.reduceByKey( min )
        val representativeMap    = keyIdRdd.join( representativeKeyRdd ).map( ( pair: ( VKey, ( VertexId, VertexId ) ) ) => pair._2 )

        val summarizedVertices = representativeMap.join( graph.vertices )
                                                  .map( ( pair: ( VertexId, ( VertexId, VD ) ) ) =>
                                                        ( pair._2._1, vertexPreAggregator( pair._2._2 ) ) )
                                                  .reduceByKey( vertexAggregator )
                                                  .map( ( vertex: ( VertexId, VDA ) ) =>
                                                        ( vertex._1, vertexPostAggregator( vertex._2 ) ) )

        val summarizedEdges = graph.edges.map( ( edge: Edge[ED] ) => ( edge.srcId, ( edge.dstId, edge.attr ) ) )
                                         .join( representativeMap )
                                         .map( ( pair: ( VertexId, ( ( VertexId, ED ), VertexId ) ) ) =>
                                               ( pair._2._1._1, ( pair._2._2, pair._2._1._2 ) ) )
                                         .join( representativeMap )
                                         .map( ( pair: ( VertexId, ( ( VertexId, ED ), VertexId ) ) ) =>
                                               ( ( pair._2._1._1, pair._2._2, edgeGroupSelector( pair._2._1._2 ) ), edgePreAggregator( pair._2._1._2 ) ) )
                                         .reduceByKey( edgeAggregator )
                                         .map( ( pair: ( ( VertexId, VertexId, EKey ), EDA ) ) =>
                                               Edge( pair._1._1, pair._1._2, edgePostAggregator( pair._2 ) ) )
*/

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
