package graph.spark.example.model.typed;

import java.time.LocalDate;

public class DeliveryNote implements Attributes
{
    private static final long serialVersionUID = 1L;

    public static final String label = "DeliveryNote";

    public final String    num;
    public final LocalDate date;
    public final String    trackingCode;

    public DeliveryNote( String num, LocalDate date, String trackingCode )
    {
        this.num          = num;
        this.date         = date;
        this.trackingCode = trackingCode;
    }

    @Override
    public String getLabel()
    {
        return label;
    }
}
