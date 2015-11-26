package graph.spark.example.model.typed;

import java.math.BigDecimal;

public class PurchOrderLine implements Attributes
{
    private static final long serialVersionUID = 1L;

    public static final String label = "PurchOrderLine";

    public final int        quantity;
    public final BigDecimal purchPrice;

    public PurchOrderLine( int quantity, BigDecimal purchPrice )
    {
        this.quantity   = quantity;
        this.purchPrice = purchPrice;
    }

    @Override
    public String getLabel()
    {
        return label;
    }
}
