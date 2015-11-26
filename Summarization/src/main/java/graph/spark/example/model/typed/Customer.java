package graph.spark.example.model.typed;

public class Customer implements Attributes
{
    private static final long serialVersionUID = 1L;

    public static final String label = "Customer";

    public final String num;
    public final String name;

    public Customer( String num, String name )
    {
        this.num  = num;
        this.name = name;
    }

    @Override
    public String getLabel()
    {
        return label;
    }
}
