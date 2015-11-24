package graph.spark.example.model.typed;

import java.time.LocalDate;

public class SalesQuotation extends Vertex
{
    private static final long serialVersionUID = 1L;

    public final String    num;
    public final LocalDate date;

    public SalesQuotation( int nodeId, String num, LocalDate date )
    {
        super( nodeId );

        this.num  = num;
        this.date = date;
    }
}
