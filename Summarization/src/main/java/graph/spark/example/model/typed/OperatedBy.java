package graph.spark.example.model.typed;

public class OperatedBy extends Edge
{
    private static final long serialVersionUID = 1L;

    public OperatedBy( int sourceId, int targetId )
    {
        super( sourceId, targetId );
    }
}
