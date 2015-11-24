package graph.spark.example.model.typed;

import java.time.LocalDate;

public class DeliveryNote extends Vertex
{
    private static final long serialVersionUID = 1L;

    public final String    num;
    public final LocalDate date;
    public final String    trackingCode;

    public DeliveryNote( int nodeId, String num, LocalDate date, String trackingCode )
    {
        super( nodeId );

        this.num          = num;
        this.date         = date;
        this.trackingCode = trackingCode;
    }
}
