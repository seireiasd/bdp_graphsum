package graph.spark.example.model.typed;

public class PartOf extends Edge
{
    private static final long serialVersionUID = 1L;

    public PartOf( int sourceId, int targetId )
    {
        super( sourceId, targetId );
    }
}
