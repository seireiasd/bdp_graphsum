package graph.spark.example.model.typed;

public class User extends Vertex
{
    private static final long serialVersionUID = 1L;

    public final String email;
    public final String name;
    public final String erpEmplNum;

    public User( int nodeId, String email, String name, String erpEmplNum )
    {
        super( nodeId );

        this.email      = email;
        this.name       = name;
        this.erpEmplNum = erpEmplNum;
    }
}
