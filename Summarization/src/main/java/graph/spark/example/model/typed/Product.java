package graph.spark.example.model.typed;

import java.math.BigDecimal;

public class Product extends Vertex
{
    private static final long serialVersionUID = 1L;

    public final String     num;
    public final String     name;
    public final String     category;
    public final BigDecimal price;

    public Product( int nodeId, String num, String name, String category, BigDecimal price )
    {
        super( nodeId );

        this.num      = num;
        this.name     = name;
        this.category = category;
        this.price    = price;
    }
}
