package graph.spark.example.model.typed;

import java.time.LocalDate;

public class SalesOrder implements Attributes
{
    private static final long serialVersionUID = 1L;

    public static final String label = "SalesOrder";

    public final String    num;
    public final LocalDate date;
    public final LocalDate deliveryDate;

    public SalesOrder( String num, LocalDate date, LocalDate deliveryDate )
    {
        this.num          = num;
        this.date         = date;
        this.deliveryDate = deliveryDate;
    }

    @Override
    public String getLabel()
    {
        return label;
    }
}
