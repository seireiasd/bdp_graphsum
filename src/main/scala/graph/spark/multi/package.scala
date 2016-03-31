package graph.spark

import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

package object multi
{
    implicit final def OperatorRdd[Key, T]( rdd: RDD[( Key, T )] ): OperatorRDD[Key, T] =
                       new OperatorRDD[Key, T]( rdd )

    private[multi] type BufferIpOps           = ( ArrayBuffer[Any], Int, Array[Operation] )
    private[multi] type KeyGroup              = ( Any, BufferIpOps )
    private[multi] type KeyGroupMap           = mutable.HashMap[Any, BufferIpOps]
    private[multi] type ShuffleKeyGroup       = ( ( Int, Any ), BufferIpOps )
    private[multi] type ShuffleKeyValue       = ( ( Int, Any ), ( Any, Int, Array[Operation] ) )
    private[multi] type PipeKeyGroup[T, U]    = ( Any, ( ArrayBuffer[Any], Int, Pipe[T, U] ) )
    private[multi] type PipeKeyGroupMap[T, U] = mutable.HashMap[Any, ( ArrayBuffer[Any], Int, Pipe[T, U] )]
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
