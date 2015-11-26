package graph.spark.example.model.generic;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class GenericAttributes implements Serializable
{
    private static final long serialVersionUID = 1L;

    public final Map<String, Object> pairs;

    public GenericAttributes()
    {
        pairs = new HashMap<String, Object>();
    }
}
