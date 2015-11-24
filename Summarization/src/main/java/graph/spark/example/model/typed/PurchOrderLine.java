package graph.spark.example.model.typed;

import java.math.BigDecimal;

public class PurchOrderLine extends Edge
{
    private static final long serialVersionUID = 1L;

    public final int        quantity;
    public final BigDecimal purchPrice;

    public PurchOrderLine( int sourceId, int targetId, int quantity, BigDecimal purchPrice )
    {
        super( sourceId, targetId );

        this.quantity   = quantity;
        this.purchPrice = purchPrice;
    }
}
