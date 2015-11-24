package graph.spark.example.model.typed;

public class Customer extends Vertex
{
    private static final long serialVersionUID = 1L;

    public final String num;
    public final String name;

    public Customer( int nodeId, String num, String name )
    {
        super( nodeId );

        this.num  = num;
        this.name = name;
    }
}
