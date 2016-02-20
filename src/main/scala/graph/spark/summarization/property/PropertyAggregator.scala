package graph.spark.summarization.property

@SerialVersionUID( 1L )
abstract class PropertyAggregator extends Serializable
{
    def preAggregate( propValue: Any ): Any

    def aggregate( p1: Any, p2: Any ): Any

    def postAggregate( aggregate: Any ): Any
}
