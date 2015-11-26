package graph.spark.example.model.typed;

public class Logistics implements Attributes
{
    private static final long serialVersionUID = 1L;

    public static final String label = "Logistics";

    public final String num;
    public final String name;

    public Logistics( String num, String name )
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
