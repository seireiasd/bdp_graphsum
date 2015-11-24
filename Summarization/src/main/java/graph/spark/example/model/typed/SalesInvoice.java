package graph.spark.example.model.typed;

import java.math.BigDecimal;
import java.time.LocalDate;

public class SalesInvoice extends Vertex
{
    private static final long serialVersionUID = 1L;

    public final String     num;
    public final LocalDate  date;
    public final BigDecimal revenue;
    public final String     text;

    public SalesInvoice( int nodeId, String num, LocalDate date, BigDecimal revenue, String text )
    {
        super( nodeId );

        this.num     = num;
        this.date    = date;
        this.revenue = revenue;
        this.text    = text;
    }
}
