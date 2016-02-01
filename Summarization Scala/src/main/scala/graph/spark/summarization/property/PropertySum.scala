package graph.spark.summarization.property

import scala.math.Numeric

object PropertySum
{
    def apply[T: Numeric](): PropertySum[T] = new PropertySum[T]()
}

class PropertySum[T: Numeric] extends PropertyAggregator
{
    def preAggregate( propValue: Any ): Any = propValue

    def aggregate( p1: Any, p2: Any ): Any = implicitly[Numeric[T]].plus( p1.asInstanceOf[T], p2.asInstanceOf[T] )

    def postAggregate( aggregate: Any ): Any = aggregate
}
