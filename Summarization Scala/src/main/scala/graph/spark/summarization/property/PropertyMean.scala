package graph.spark.summarization.property

import scala.math.Numeric

object PropertyMean
{
    def apply[T: Numeric](): PropertyMean[T] = new PropertyMean[T]()
}

class PropertyMean[T: Numeric] extends PropertyAggregator
{
    def preAggregate( propValue: Any ): Any = ( propValue, 1l )

    def aggregate( p1: Any, p2: Any ): Any =
    {
        val ( v1, c1 ) = p1.asInstanceOf[( T, Long )]
        val ( v2, c2 ) = p2.asInstanceOf[( T, Long )]

        ( implicitly[Numeric[T]].plus( v1, v2 ), c1 + c2 )
    }

    def postAggregate( aggregate: Any ): Any =
    {
        val ( v, c ) = aggregate.asInstanceOf[( T, Long )]

        v match
        {
            case v: Int        => ( v / c ).toInt
            case v: Long       => ( v / c )
            case v: Float      => ( v / c )
            case v: Double     => ( v / c )
            case v: BigDecimal => ( v / c )
            case _             => ( implicitly[Numeric[T]].toDouble( v ) / c )
        }
    }
}
