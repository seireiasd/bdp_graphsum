package graph.spark.example.model.generic;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class GenericVertex implements Serializable
{
    private static final long serialVersionUID = 1L;

    public final Map<String, Object> attributes;
    public final int                 nodeId;

    public GenericVertex( int nodeId )
    {
        this.attributes = new HashMap<String, Object>();
        this.nodeId     = nodeId;
    }
}
