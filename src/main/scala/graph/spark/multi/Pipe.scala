package graph.spark.multi

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

class Pipe[U, V] private ( private[multi] val processors: Array[Operation], private[multi] val boundaries: Int ) extends Serializable
{
    def ->[W]( next: Pipe[V, W] ): Pipe[U, W] =
        new Pipe[U, W]( processors ++ next.processors, boundaries + next.boundaries )
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

object Pipe
{
    def apply[T](): Pipe[T, T] =
        apply[T, T]( Array[Operation](), 0 )

    private[multi] def apply[U, V]( processors: Array[Operation], boundaries: Int ): Pipe[U, V] =
                       new Pipe[U, V]( processors, boundaries )
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

object Mapper
{
    def apply[U, V]( map: U => V ): Pipe[U, V] =
        Pipe[U, V]( Array( new MapOperation( map.asInstanceOf[Any => Any] ) ), 0 )
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

object Reducer
{
    def apply[T]( reduce: ( T, T ) => T ): Pipe[T, T] =
        Pipe[T, T]( Array( new ReduceOperation( reduce.asInstanceOf[( Any, Any ) => Any] ) ), 1 )
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

object Partitioner
{
    def apply[U, V]( assign: U => Any, pipe: Pipe[U, V] ): Pipe[U, V] =
        Pipe[U, V]( Array( new PartitionedPushOperation( assign.asInstanceOf[Any => Any], pipe.processors ) ), pipe.boundaries )
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

object Parallel
{
    def apply[U, V]( pipes: Array[Pipe[U, V]] ): Pipe[U, V] =
    {
        require( pipes.nonEmpty )

        val max = pipes.maxBy( pipe => pipe.boundaries ).boundaries

        Pipe[U, V]( Array( new ParallelPushOperation( pipes.map( pipe => pipe.processors :+ new WaitOperation( max - pipe.boundaries ) ) ) ), max + 1 )
    }
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
