package graph.spark.example.model.typed;

public class OpenedBy extends Edge
{
    private static final long serialVersionUID = 1L;

    public OpenedBy( int sourceId, int targetId )
    {
        super( sourceId, targetId );
    }
}
