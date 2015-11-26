package graph.spark.example.model.typed;

import java.math.BigDecimal;

public class Product implements Attributes
{
    private static final long serialVersionUID = 1L;

    public static final String label = "Product";

    public final String     num;
    public final String     name;
    public final String     category;
    public final BigDecimal price;

    public Product( String num, String name, String category, BigDecimal price )
    {
        this.num      = num;
        this.name     = name;
        this.category = category;
        this.price    = price;
    }

    @Override
    public String getLabel()
    {
        return label;
    }
}
