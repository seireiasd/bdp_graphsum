package graph.spark.example

import graph.spark.multi.{Mapper, Partitioned, Pipe, Reducer}

import scala.math.Numeric

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

object CountAll
{
    def apply[T](): Pipe[T, Long] =
        Mapper[T, Long]( _ => 1L ) -> Reducer[Long]( ( c1: Long, c2: Long ) => c1 + c2 )
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

object Count
{
    def apply[T](): Pipe[T, Long] =
        Partitioned( identity[T], Reducer[T]( ( t1, t2 ) => t1 ) ) -> CountAll[T]()
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

object Sum
{
    def apply[T: Numeric](): Pipe[T, T] =
        Reducer[T]( ( c1: T, c2: T ) => implicitly[Numeric[T]].plus( c1, c2 ) )
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

object Max
{
    def apply[T: Numeric](): Pipe[T, T] =
        Reducer[T]( ( c1: T, c2: T ) => implicitly[Numeric[T]].max( c1, c2 ) )
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

object SelectAttribute
{
    def apply[T]( name: String ): Pipe[Attributes, T] =
        Mapper[Attributes, T]( x => x.property[T]( name ) )
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

object ToKeyValue
{
    def apply[T]( name: String ): Pipe[T, ( String, Any )] =
        Mapper[T, ( String, Any )]( x => ( name, x ) )
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/

object CollectAttributes
{
    def apply( label: String ): Pipe[( String, Any ), Attributes] =
        Mapper[( String, Any ), Map[String, Any]]( x => Map( x ) ) ->
        Reducer[Map[String, Any]]( ( m1, m2 ) => m1 ++ m2 ) ->
        Mapper[Map[String, Any], Attributes]( x => Attributes( label, x ) )
}

/*----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------*/
