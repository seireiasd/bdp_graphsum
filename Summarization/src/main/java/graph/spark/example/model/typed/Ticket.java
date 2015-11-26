package graph.spark.example.model.typed;

import java.time.LocalDate;

public class Ticket implements Attributes
{
    private static final long serialVersionUID = 1L;

    public static final String label = "Ticket";

    public final int       id;
    public final LocalDate createdAt;
    public final String    problem;
    public final String    erpSoNum;

    public Ticket( int id, LocalDate createdAt, String problem, String erpSoNum )
    {
        this.id        = id;
        this.createdAt = createdAt;
        this.problem   = problem;
        this.erpSoNum  = erpSoNum;
    }

    @Override
    public String getLabel()
    {
        return label;
    }
}
