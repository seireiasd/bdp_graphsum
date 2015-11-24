package graph.spark.example.model.typed;

import java.time.LocalDate;

public class SalesOrder extends Vertex
{
    private static final long serialVersionUID = 1L;

    public final String    num;
    public final LocalDate date;
    public final LocalDate deliveryDate;

    public SalesOrder( int nodeId, String num, LocalDate date, LocalDate deliveryDate )
    {
        super( nodeId );

        this.num          = num;
        this.date         = date;
        this.deliveryDate = deliveryDate;
    }
}
