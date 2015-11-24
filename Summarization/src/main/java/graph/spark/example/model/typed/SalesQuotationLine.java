package graph.spark.example.model.typed;

import java.math.BigDecimal;

public class SalesQuotationLine extends Edge
{
    private static final long serialVersionUID = 1L;

    public final int        quantity;
    public final BigDecimal salesPrice;
    public final BigDecimal purchPrice;

    public SalesQuotationLine( int sourceId, int targetId, int quantity, BigDecimal salesPrice, BigDecimal purchPrice )
    {
        super( sourceId, targetId );

        this.quantity   = quantity;
        this.salesPrice = salesPrice;
        this.purchPrice = purchPrice;
    }
}
