package graph.spark.example.model.typed;

public class Employee implements Attributes
{
    private static final long serialVersionUID = 1L;

    public static final String label = "Employee";

    public final String num;
    public final String name;
    public final String gender;

    public Employee( String num, String name, String gender )
    {
        this.num    = num;
        this.name   = name;
        this.gender = gender;
    }

    @Override
    public String getLabel()
    {
        return label;
    }
}
