package graph.spark.multi

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

class OperatorRDD[Key, T]( rdd: RDD[( Key, T )] )
{
    def runOperations[U]( operator: Key => Pipe[T, U] ): RDD[( Key, U )] =
    {
        import OperatorRDD._

        val assigned    = rdd.mapPartitions( initialize( operator ) )
        val iterations  = assigned.aggregate( 0 )( { case ( num, ( _, ( _, _, pipe ) ) ) => scala.math.max( num, pipe.boundaries ) }, scala.math.max )
        var input       = assigned.mapPartitionsWithIndex( ( idx, it ) => runStage( idx, it.map( { case ( key, ( values, ip, pipe ) ) => ( key, ( values, ip, pipe.processors ) ) } ) ) )
        val partitioner = new BoundaryPartitioner( input.partitions.length )

        for ( i <- 1 to iterations )
        {
            input = input.partitionBy( partitioner )
                         .mapPartitionsWithIndex( ( idx, it ) => runStage( idx, runPostShuffle( it ) ) )
        }

        input.flatMap( { case ( ( idx, key ), ( values, ip, ops ) ) => values.asInstanceOf[ArrayBuffer[Any]].map( value => ( key.asInstanceOf[Key], value.asInstanceOf[U] ) ) } )
    }
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

private[multi] object OperatorRDD
{
    class BoundaryPartitioner( partitions: Int ) extends Partitioner
    {
        override def numPartitions: Int =
                     partitions

        /*--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

        override def getPartition( key: Any ): Int =
        {
            val ( idx, groupKey ) = key.asInstanceOf[( Int, Any )]

            if ( idx < 0 )
            {
                computePartition( groupKey )
            }
            else
            {
                idx
            }
        }

        /*--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

        override def equals( other: Any ): Boolean =
        {
            other match
            {
                case p: BoundaryPartitioner => p.numPartitions == numPartitions
                case _                      => false
            }
        }

        /*--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

        override def hashCode: Int =
                     numPartitions

        /*--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

        private def computePartition( key: Any ): Int =
        {
            val mod = key.hashCode() % numPartitions

            if ( mod < 0 )
            {
                mod + numPartitions
            }
            else
            {
                mod
            }
        }
    }

    /*------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

    def initialize[Key, T, U]( operator: Key => Pipe[T, U] )( it: Iterator[( Key, T )] ): Iterator[PipeKeyGroup[T, U]] =
    {
        val result = new PipeKeyGroupMap[T, U]()

        while ( it.hasNext )
        {
            val ( key, value ) = it.next()

            result.getOrElseUpdate( key, ( ArrayBuffer[Any](), 0, operator( key ) ) )._1 += value
        }

        result.iterator
    }

    /*------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

    def runStage( idx: Int, it: Iterator[KeyGroup] ): Iterator[ShuffleKeyValue] =
    {
        var elems     = it
        val completed = new ArrayBuffer[ShuffleKeyValue]()

        while ( elems.nonEmpty )
        {
            val result = new KeyGroupMap()

            while ( elems.hasNext )
            {
                val elem                = elems.next()
                val ( key, valueIpOps ) = elem
                val ( _, ip, ops )      = valueIpOps

                if ( ip == ops.length )
                {
                    completed += ( ( ( idx, key ), valueIpOps ) )
                }
                else
                {
                    ops( ip ) match
                    {
                        case op: StageOperation    => op.execute( elem, result )
                        case op: BoundaryOperation => op.preShuffle( ( ( idx, key ), valueIpOps ), completed )
                    }
                }
            }

            elems = result.iterator
        }

        completed.iterator
    }

    /*------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

    def runPostShuffle( it: Iterator[ShuffleKeyValue] ): Iterator[KeyGroup] =
    {
        val collected = new KeyGroupMap()
        val completed = new ArrayBuffer[KeyGroup]()

        while ( it.hasNext )
        {
            val ( ( _, key ), ( value, ip, ops ) ) = it.next()

            if ( ip == ops.length )
            {
                completed += ( ( key, ( value.asInstanceOf[ArrayBuffer[Any]], ip, ops ) ) )
            }
            else
            {
                collected.getOrElseUpdate( key, ( ArrayBuffer[Any](), ip, ops ) )._1 += value
            }
        }

        collected.foreach( elem =>
                           {
                               val ( _, ( _, ip, ops ) ) = elem

                               ops( ip ).asInstanceOf[BoundaryOperation].postShuffle( elem, completed )
                           } )

        completed.iterator
    }
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
