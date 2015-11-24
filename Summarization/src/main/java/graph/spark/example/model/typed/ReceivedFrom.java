package graph.spark.example.model.typed;

public class ReceivedFrom extends Edge
{
    private static final long serialVersionUID = 1L;

    public ReceivedFrom( int sourceId, int targetId )
    {
        super( sourceId, targetId );
    }
}
