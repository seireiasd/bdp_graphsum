package graph.spark.summarization

import graph.spark.multi.{OperatorRdd, Pipe}
import org.apache.spark.graphx.{Edge, Graph}

import scala.math.min
import scala.reflect.ClassTag

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

object GraphSummarizer
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
                                        .map( { case ( key, ( oldVertexId, newVertexId ) ) => ( oldVertexId, newVertexId ) } ).cache()

        // assign group vertex ids and aggregate vertex groups

        val summarizedVertices = representativeMap.join( graph.vertices )
                                                  .map( { case ( oldVertexId, ( newVertexId, data ) ) => ( newVertexId, vertexPreAggregator( data ) ) } )
                                                  .reduceByKey( vertexAggregator )
                                                  .map( { case ( vertexId, data ) => ( vertexId, vertexPostAggregator( data ) ) } )

        // connect edges to their respective aggregated vertices, select edge groups and aggregate edge groups

        val summarizedEdges = graph.edges.map( ( edge: Edge[ED] ) => ( edge.srcId, ( edge.dstId, edge.attr ) ) )
                                         .join( representativeMap )
                                         .map( { case ( oldSourceId, ( ( oldTargetId, data ), newSourceId ) ) => ( oldTargetId, ( newSourceId, data ) ) } )
                                         .join( representativeMap )
                                         .map( { case ( oldTargetId, ( ( newSourceId, data ), newTargetId ) ) =>
                                                      ( ( newSourceId, newTargetId, edgeGroupSelector( data ) ), edgePreAggregator( data ) ) } )
                                         .reduceByKey( edgeAggregator )
                                         .map( { case ( ( sourceId, targetId, key ), data ) => Edge( sourceId, targetId, edgePostAggregator( data ) ) } )

        representativeMap.unpersist()

        // return summarized graph

        Graph( summarizedVertices, summarizedEdges )
    }

    /*------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

    def apply[VD, ED, VKey: ClassTag, EKey: ClassTag, VDS: ClassTag, EDS: ClassTag]
             ( graph:               Graph[VD, ED],
               vertexGroupSelector: VD => VKey,
               vertexAggregator:    VKey => Pipe[VD, VDS],
               edgeGroupSelector:   ED => EKey,
               edgeAggregator:      EKey => Pipe[ED, EDS] ):
        Graph[VDS, EDS] =
    {
        val assignedVertices = graph.vertices.map( { case ( vertexId, data ) => ( vertexGroupSelector( data ), data ) } )
        val idKeyRdd         = graph.vertices.map( { case ( vertexId, data ) => ( vertexId, vertexGroupSelector( data ) ) } )
        val assignedEdges    = graph.edges.map( edge => ( edge.srcId, ( edge.dstId, edge.attr ) ) )
                                          .join( idKeyRdd )
                                          .map( { case ( oldSourceId, ( ( oldTargetId, data ), newSourceKey ) ) => ( oldTargetId, ( newSourceKey, data ) ) } )
                                          .join( idKeyRdd )
                                          .map( { case ( oldTargetId, ( ( newSourceKey, data ), newTargetKey ) ) => ( ( newSourceKey, newTargetKey, edgeGroupSelector( data ) ), data ) } )

        val representativeKeyMap = graph.vertices.map( { case ( vertexId, data ) => ( vertexGroupSelector( data ), vertexId ) } ).reduceByKey( min )
        val processedVertices    = assignedVertices.runOperations( vertexAggregator )
        val summarizedVertices   = representativeKeyMap.join( processedVertices )
                                                       .map( { case ( key, ( vertexId, data ) ) => ( vertexId, data ) } )

        val processedEdges  = assignedEdges.runOperations( { case ( sourceKey, targetKey, key ) => edgeAggregator( key ) } )
        val summarizedEdges = processedEdges.map( { case ( ( sourceKey, targetKey, key ), data ) => ( sourceKey, ( targetKey, data ) ) } )
                                            .join( representativeKeyMap )
                                            .map( { case ( sourceKey, ( ( targetKey, data ), newSourceId ) ) => ( targetKey, ( newSourceId, data ) ) } )
                                            .join( representativeKeyMap )
                                            .map( { case ( targetKey, ( ( newSourceId, data ), newTargetId ) ) => Edge( newSourceId, newTargetId, data ) } )

        Graph( summarizedVertices, summarizedEdges )
    }
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
