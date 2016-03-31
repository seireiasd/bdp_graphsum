package graph.spark.multi

import scala.collection.mutable.ArrayBuffer

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

private[multi] trait Operation extends Serializable

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

private[multi] trait StageOperation extends Operation
{
    def execute( group: KeyGroup, result: KeyGroupMap ): Unit
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

private[multi] trait BoundaryOperation extends Operation
{
    def preShuffle( group: ShuffleKeyGroup, result: ArrayBuffer[ShuffleKeyValue] ): Unit

    def postShuffle( group: KeyGroup, result: ArrayBuffer[KeyGroup] ): Unit
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

private[multi] class MapOperation( val f: Any => Any ) extends StageOperation
{
    override def execute( group: KeyGroup, result: KeyGroupMap ): Unit =
    {
        val ( key, ( values, ip, ops ) ) = group

        result.update( key, ( values.map( f ), ip + 1, ops ) )
    }
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

private[multi] class PartitionedPushOperation( val f: Any => Any, val pipe: Array[Operation] ) extends StageOperation
{
    override def execute( group: KeyGroup, result: KeyGroupMap ): Unit =
    {
        val ( key, ( values, ip, ops ) ) = group

        values.foreach( value => result.getOrElseUpdate( ( key, f( value ) ), ( ArrayBuffer[Any](), 0, pipe :+ new PopOperation( ip + 1, ops ) ) )._1 += value )
    }
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

private[multi] class PopOperation( val ip: Int, val pipe: Array[Operation] ) extends StageOperation
{
    override def execute( group: KeyGroup, result: KeyGroupMap ): Unit =
    {
        val ( ( key, _ ), ( values, _, _ ) ) = group.asInstanceOf[( ( Any, Any ), BufferIpOps )]

        result.getOrElseUpdate( key, ( ArrayBuffer[Any](), ip, pipe ) )._1 ++= values
    }
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

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
