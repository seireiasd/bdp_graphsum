package graph.spark.example.model.typed;

public class Employee extends Vertex
{
    private static final long serialVersionUID = 1L;

    public final String num;
    public final String name;
    public final String gender;

    public Employee( int nodeId, String num, String name, String gender )
    {
        super( nodeId );

        this.num    = num;
        this.name   = name;
        this.gender = gender;
    }
}
