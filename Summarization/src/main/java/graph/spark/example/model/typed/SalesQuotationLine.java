package graph.spark.example.model.typed;

import java.math.BigDecimal;

public class SalesQuotationLine implements Attributes
{
    private static final long serialVersionUID = 1L;

    public static final String label = "SalesQuotationLine";

    public final int        quantity;
    public final BigDecimal salesPrice;
    public final BigDecimal purchPrice;

    public SalesQuotationLine( int quantity, BigDecimal salesPrice, BigDecimal purchPrice )
    {
        this.quantity   = quantity;
        this.salesPrice = salesPrice;
        this.purchPrice = purchPrice;
    }

    @Override
    public String getLabel()
    {
        return label;
    }
}
