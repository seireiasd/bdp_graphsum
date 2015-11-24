package graph.spark.example.model.typed;

public class AllocatedTo extends Edge
{
    private static final long serialVersionUID = 1L;

    public AllocatedTo( int sourceId, int targetId )
    {
        super( sourceId, targetId );
    }
}
