package graph.spark.example.model.typed;

import java.time.LocalDate;

public class Ticket extends Vertex
{
    private static final long serialVersionUID = 1L;

    public final int       id;
    public final LocalDate createdAt;
    public final String    problem;
    public final String    erpSoNum;

    public Ticket( int nodeId, int id, LocalDate createdAt, String problem, String erpSoNum )
    {
        super( nodeId );

        this.id        = id;
        this.createdAt = createdAt;
        this.problem   = problem;
        this.erpSoNum  = erpSoNum;
    }
}
