package graph.spark.example.model.typed;

import java.math.BigDecimal;
import java.time.LocalDate;

public class PurchInvoice extends Vertex
{
    private static final long serialVersionUID = 1L;

    public final String     num;
    public final LocalDate  date;
    public final BigDecimal expense;
    public final String     text;

    public PurchInvoice( int nodeId, String num, LocalDate date, BigDecimal expense, String text )
    {
        super( nodeId );

        this.num     = num;
        this.date    = date;
        this.expense = expense;
        this.text    = text;
    }
}
