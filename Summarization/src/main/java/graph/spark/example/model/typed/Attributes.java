package graph.spark.example.model.typed;

import java.io.Serializable;

public interface Attributes extends Serializable
{
    public String getLabel();

    @SuppressWarnings( "unchecked" )
    default public <T> T cast()
    {
        return ( T ) this;
    }
}
