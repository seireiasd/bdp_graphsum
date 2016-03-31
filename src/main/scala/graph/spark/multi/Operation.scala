package graph.spark.multi

import scala.collection.mutable.ArrayBuffer

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

/**
  * Represents low-level operations that's agnostic of input and output data types. Like actual Spark transformations they are separated into stage and boundary operations.
  */
private[multi] trait Operation extends Serializable

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

/**
  * Stage operations can be run on single elements or partitions respectively.
  */
private[multi] trait StageOperation extends Operation
{
    /**
      * Performs the actual spark transformations. Outputs are to be grouped according to their keys.
      *
      * @param group  the input element
      * @param result the output data set
      */
    def execute( group: KeyGroup, result: KeyGroupMap ): Unit
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

/**
  * Boundary operations run between stages and may require shuffling.
  */
private[multi] trait BoundaryOperation extends Operation
{
    /**
      * Runs subsequently to a stage and before the shuffling step.
      *
      * @param group  input element, whose key has been supplemented by the local partition's index
      * @param result the output data set, partition indices indicate the target partition, elements with negative index will be hash partitioned
      */
    def preShuffle( group: ShuffleKeyGroup, result: ArrayBuffer[ShuffleKeyValue] ): Unit

    /**
      * Runs after the shuffling step and before the next stage.
      *
      * @param group  input element containing all values of equal key created in the preShuffle step ( residing on the local partition )
      * @param result the output data set
      */
    def postShuffle( group: KeyGroup, result: ArrayBuffer[KeyGroup] ): Unit
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

/**
  * Performs a map operation.
  *
  * @param f the map function
  */
private[multi] class MapOperation( val f: Any => Any ) extends StageOperation
{
    override def execute( group: KeyGroup, result: KeyGroupMap ): Unit =
    {
        val ( key, ( values, ip, ops ) ) = group

        result.update( key, ( values.map( f ), ip + 1, ops ) )
    }
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

/**
  * Subpartitions a group and executes an operator sequence on it.
  *
  * @param f    assigns a subgroup to each element
  * @param pipe operations to be executed within each partition
  */
private[multi] class PartitionedPushOperation( val f: Any => Any, val pipe: Array[Operation] ) extends StageOperation
{
    override def execute( group: KeyGroup, result: KeyGroupMap ): Unit =
    {
        val ( key, ( values, ip, ops ) ) = group

        values.foreach( value => result.getOrElseUpdate( ( key, f( value ) ), ( ArrayBuffer[Any](), 0, pipe :+ new PopOperation( ip + 1, ops ) ) )._1 += value )
    }
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

/**
  * Recreates the previous grouping and operator sequence.
  *
  * @param ip   the former instruction pointer
  * @param pipe the former operator sequence
  */
private[multi] class PopOperation( val ip: Int, val pipe: Array[Operation] ) extends StageOperation
{
    override def execute( group: KeyGroup, result: KeyGroupMap ): Unit =
    {
        val ( ( key, _ ), ( values, _, _ ) ) = group.asInstanceOf[( ( Any, Any ), BufferIpOps )]

        result.getOrElseUpdate( key, ( ArrayBuffer[Any](), ip, pipe ) )._1 ++= values
    }
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

/**
  * Performs a reduce operation.
  *
  * @param reduce the reduce function
  */
private[multi] class ReduceOperation( val reduce: ( Any, Any ) => Any ) extends BoundaryOperation
{
    override def preShuffle( group: ShuffleKeyGroup, result: ArrayBuffer[ShuffleKeyValue] ): Unit =
    {
        val ( ( _, key ), ( values, ip, ops ) ) = group

        result += ( ( ( -1, key ), ( values.reduce( reduce ), ip, ops ) ) )
    }

    override def postShuffle( group: KeyGroup, result: ArrayBuffer[KeyGroup] ): Unit =
    {
        val ( key, ( values, ip, ops ) ) = group

        result += ( ( key, ( ArrayBuffer( values.reduce( reduce ) ), ip + 1, ops ) ) )
    }
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

/**
  * Sets up the input data to be executed by multiple pipes in parallel.
  *
  * @param pipes the pipes to execute
  */
private[multi] class ParallelPushOperation( val pipes: Array[Array[Operation]] ) extends StageOperation
{
    override def execute( group: KeyGroup, result: KeyGroupMap ): Unit =
    {
        val ( key, ( values, ip, ops ) ) = group
        var thread                       = 0

        while ( thread < pipes.length )
        {
            result.update( ( key, thread ), ( values, 0, pipes( thread ) :+ new PopOperation( ip + 1, ops ) ) )

            thread += 1
        }
    }
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

/**
  * Waiting operation used to synchronize the integration time of parallel executed pipes.
  *
  * @param deferment continuation timer
  */
private[multi] class WaitOperation( val deferment: Int ) extends BoundaryOperation
{
    override def preShuffle( group: ShuffleKeyGroup, result: ArrayBuffer[ShuffleKeyValue] ): Unit =
    {
        result += group
    }

    override def postShuffle( group: KeyGroup, result: ArrayBuffer[KeyGroup] ): Unit =
    {
        val ( key, ( values, ip, ops ) ) = group

        if ( deferment == 0 )
        {
            result += ( ( key, ( values( 0 ).asInstanceOf[ArrayBuffer[Any]], ip + 1, ops ) ) )
        }
        else
        {
            ops( ip ) = new WaitOperation( deferment - 1 )

            result += ( ( key, ( values( 0 ).asInstanceOf[ArrayBuffer[Any]], ip, ops ) ) )
        }
    }
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
