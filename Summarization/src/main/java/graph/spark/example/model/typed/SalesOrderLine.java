package graph.spark.example.model.typed;

import java.math.BigDecimal;

public class SalesOrderLine extends Edge
{
    private static final long serialVersionUID = 1L;

    public final int        quantity;
    public final BigDecimal salesPrice;

    public SalesOrderLine( int sourceId, int targetId, int quantity, BigDecimal salesPrice )
    {
        super( sourceId, targetId );

        this.quantity   = quantity;
        this.salesPrice = salesPrice;
    }
}
