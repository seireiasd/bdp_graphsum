package graph.spark.example.model.typed;

import java.io.Serializable;

public abstract class Edge implements Serializable
{
    private static final long serialVersionUID = 1L;

    public final int sourceId;
    public final int targetId;

    public Edge( int sourceId, int targetId )
    {
        this.sourceId = sourceId;
        this.targetId = targetId;
    }
}
