package graph.spark.example.model.typed;

import java.math.BigDecimal;

public class SalesOrderLine implements Attributes
{
    private static final long serialVersionUID = 1L;

    public static final String label = "SalesOrderLine";

    public final int        quantity;
    public final BigDecimal salesPrice;

    public SalesOrderLine( int quantity, BigDecimal salesPrice )
    {
        this.quantity   = quantity;
        this.salesPrice = salesPrice;
    }

    @Override
    public String getLabel()
    {
        return label;
    }
}
