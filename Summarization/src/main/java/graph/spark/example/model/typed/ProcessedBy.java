package graph.spark.example.model.typed;

public class ProcessedBy extends Edge
{
    private static final long serialVersionUID = 1L;

    public ProcessedBy( int sourceId, int targetId )
    {
        super( sourceId, targetId );
    }
}
