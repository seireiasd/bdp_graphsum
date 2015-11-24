package graph.spark.example.model.typed;

public class SentBy extends Edge
{
    private static final long serialVersionUID = 1L;

    public SentBy( int sourceId, int targetId )
    {
        super( sourceId, targetId );
    }
}
