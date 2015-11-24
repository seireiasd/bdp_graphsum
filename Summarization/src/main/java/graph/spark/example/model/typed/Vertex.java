package graph.spark.example.model.typed;

import java.io.Serializable;

public abstract class Vertex implements Serializable
{
    private static final long serialVersionUID = 1L;

    public final int nodeId;

    public Vertex( int nodeId )
    {
        this.nodeId = nodeId;
    }
}
