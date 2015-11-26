package graph.spark.example.model.typed;

import java.math.BigDecimal;
import java.time.LocalDate;

public class PurchInvoice implements Attributes
{
    private static final long serialVersionUID = 1L;

    public static final String label = "PurchInvoice";

    public final String     num;
    public final LocalDate  date;
    public final BigDecimal expense;
    public final String     text;

    public PurchInvoice( String num, LocalDate date, BigDecimal expense, String text )
    {
        this.num     = num;
        this.date    = date;
        this.expense = expense;
        this.text    = text;
    }

    @Override
    public String getLabel()
    {
        return label;
    }
}
