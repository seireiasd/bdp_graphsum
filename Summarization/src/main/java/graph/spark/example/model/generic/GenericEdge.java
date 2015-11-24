package graph.spark.example.model.generic;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class GenericEdge implements Serializable
{
    private static final long serialVersionUID = 1L;

    public final Map<String, Object> attributes;
    public final int                 sourceId;
    public final int                 targetId;

    public GenericEdge( int sourceId, int targetId )
    {
        this.attributes = new HashMap<String, Object>();
        this.sourceId   = sourceId;
        this.targetId   = targetId;
    }
}
