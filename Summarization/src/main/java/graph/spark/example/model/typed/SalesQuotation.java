package graph.spark.example.model.typed;

import java.time.LocalDate;

public class SalesQuotation implements Attributes
{
    private static final long serialVersionUID = 1L;

    public static final String label = "SalesQuotation";

    public final String    num;
    public final LocalDate date;

    public SalesQuotation( String num, LocalDate date )
    {
        this.num  = num;
        this.date = date;
    }

    @Override
    public String getLabel()
    {
        return label;
    }
}
