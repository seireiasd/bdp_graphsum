package graph.spark.multi

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

/**
  * Represents a Spark operator sequence.
  *
  * @param processors type-agnostic low-level operations to execute
  * @param boundaries number of boundary operations within the sequence
  * @tparam U         the input data type
  * @tparam V         the output data type
  */
class Pipe[-U, +V] private ( private[multi] val processors: Array[Operation], private[multi] val boundaries: Int ) extends Serializable
{
    /**
      * Links pipes with compatible input and output formats.
      *
      * @param next the pipe to append
      * @tparam W   the output data type of 'next'
      * @return     the linked pipe
      */
    def ->[W]( next: Pipe[V, W] ): Pipe[U, W] =
        new Pipe[U, W]( processors ++ next.processors, boundaries + next.boundaries )
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

/**
  * Factory object for the Pipe class.
  */
object Pipe
{
    /**
      * Creates an empty pipe.
      *
      * @tparam T the input and output data type
      * @return   a no-op pipe
      */
    def apply[T](): Pipe[T, T] =
        apply[T, T]( Array[Operation](), 0 )

    /**
      * Creates a pipe.
      *
      * @param processors type-agnostic low-level operations to execute
      * @param boundaries number of boundary operations within the sequence
      * @tparam U         the input data type
      * @tparam V         the output data type
      * @return           a mighty pipe
      */
    private[multi] def apply[U, V]( processors: Array[Operation], boundaries: Int ): Pipe[U, V] =
                       new Pipe[U, V]( processors, boundaries )
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

/**
  * Represents a Spark map operation.
  */
object Mapper
{
    /**
      * Creates a pipe that performs a map operation.
      *
      * @param map the map function
      * @tparam U  the input data type
      * @tparam V  the output data type
      * @return    guess what...
      */
    def apply[U, V]( map: U => V ): Pipe[U, V] =
        Pipe[U, V]( Array( new MapOperation( map.asInstanceOf[Any => Any] ) ), 0 )
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

/**
  * Represents a Spark reduce operation over a group.
  */
object Reducer
{
    /**
      * Creates a pipe that performs a reduce operation.
      *
      * @param reduce the reduce function
      * @tparam T     the input and output data type
      * @return       a tousled tribble...
      */
    def apply[T]( reduce: ( T, T ) => T ): Pipe[T, T] =
        Pipe[T, T]( Array( new ReduceOperation( reduce.asInstanceOf[( Any, Any ) => Any] ) ), 1 )
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

/**
  * Subpartitions a group and executes a series of operations for each independently.
  */
object Partitioned
{
    /**
      * Creates a pipe that subpartitions the data set, executes an operator sequence and finally recombines it.
      *
      * @param assign assigns a subgroup to each element
      * @param pipe   operations to be executed within each partition
      * @tparam U     the input data type
      * @tparam V     the output data type
      * @return       a cake...
      */
    def apply[U, V]( assign: U => Any, pipe: Pipe[U, V] ): Pipe[U, V] =
        Pipe[U, V]( Array( new PartitionedPushOperation( assign.asInstanceOf[Any => Any], pipe.processors ) ), pipe.boundaries )
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

/**
  * Runs a number of pipes in parallel on the input data. The pipes are processed independently und eventually combined.
  */
object Parallel
{
    /**
      * Creates a pipe that runs multiple ( at least one ) operator sequences in parallel.
      *
      * @param pipes the pipes to execute
      * @tparam U    the input data type
      * @tparam V    the output data type
      * @return      the cake is a lie
      */
    def apply[U, V]( pipes: Array[Pipe[U, V]] ): Pipe[U, V] =
    {
        require( pipes.nonEmpty )

        val max = pipes.maxBy( pipe => pipe.boundaries ).boundaries

        Pipe[U, V]( Array( new ParallelPushOperation( pipes.map( pipe => pipe.processors :+ new WaitOperation( max - pipe.boundaries ) ) ) ), max + 1 )
    }
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
