package graph.spark.example.model.typed;

public class SentTo extends Edge
{
    private static final long serialVersionUID = 1L;

    public SentTo( int sourceId, int targetId )
    {
        super( sourceId, targetId );
    }
}
