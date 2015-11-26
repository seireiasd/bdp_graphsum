package graph.spark.example.model.typed;

import java.math.BigDecimal;
import java.time.LocalDate;

public class SalesInvoice implements Attributes
{
    private static final long serialVersionUID = 1L;

    public static final String label = "SalesInvoice";

    public final String     num;
    public final LocalDate  date;
    public final BigDecimal revenue;
    public final String     text;

    public SalesInvoice( String num, LocalDate date, BigDecimal revenue, String text )
    {
        this.num     = num;
        this.date    = date;
        this.revenue = revenue;
        this.text    = text;
    }

    @Override
    public String getLabel()
    {
        return label;
    }
}
