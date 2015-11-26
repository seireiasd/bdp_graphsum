package graph.spark.example.model.typed;

public class Vendor implements Attributes
{
    private static final long serialVersionUID = 1L;

    public static final String label = "Vendor";

    public final String num;
    public final String name;

    public Vendor( String num, String name )
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
