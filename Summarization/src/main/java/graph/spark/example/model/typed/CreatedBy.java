package graph.spark.example.model.typed;

public class CreatedBy extends Edge
{
    private static final long serialVersionUID = 1L;

    public CreatedBy( int sourceId, int targetId )
    {
        super( sourceId, targetId );
    }
}
