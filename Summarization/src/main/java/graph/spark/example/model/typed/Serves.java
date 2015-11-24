package graph.spark.example.model.typed;

public class Serves extends Edge
{
    private static final long serialVersionUID = 1L;

    public Serves( int sourceId, int targetId )
    {
        super( sourceId, targetId );
    }
}
