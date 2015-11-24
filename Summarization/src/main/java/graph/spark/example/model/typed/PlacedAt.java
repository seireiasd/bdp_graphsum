package graph.spark.example.model.typed;

public class PlacedAt extends Edge
{
    private static final long serialVersionUID = 1L;

    public PlacedAt( int sourceId, int targetId )
    {
        super( sourceId, targetId );
    }
}
