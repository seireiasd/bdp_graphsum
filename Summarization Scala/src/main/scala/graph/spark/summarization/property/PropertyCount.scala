package graph.spark.summarization.property

object PropertyCount
{
    def apply(): PropertyCount = new PropertyCount()
}

class PropertyCount extends PropertyAggregator
{
    def preAggregate( propValue: Any ): Any = 1l

    def aggregate( p1: Any, p2: Any ): Any = { p1.asInstanceOf[Long] + p2.asInstanceOf[Long] }

    def postAggregate( aggregate: Any ): Any = aggregate
}
