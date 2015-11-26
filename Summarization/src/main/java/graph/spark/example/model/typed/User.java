package graph.spark.example.model.typed;

public class User implements Attributes
{
    private static final long serialVersionUID = 1L;

    public static final String label = "User";

    public final String email;
    public final String name;
    public final String erpEmplNum;

    public User( String email, String name, String erpEmplNum )
    {
        this.email      = email;
        this.name       = name;
        this.erpEmplNum = erpEmplNum;
    }

    @Override
    public String getLabel()
    {
        return label;
    }
}
