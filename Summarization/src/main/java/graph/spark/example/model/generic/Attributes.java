package graph.spark.example.model.generic;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Attributes implements Serializable
{
    private static final long serialVersionUID = 1L;

    private final Map<String, Object> mProperties;
    private String                    mLabel;

    public Attributes()
    {
        mProperties = new HashMap<String, Object>();
    }

    public Attributes( String label )
    {
        mProperties = new HashMap<String, Object>();
        mLabel      = label;
    }

    public Object getProperty( String key )
    {
        return mProperties.get( key );
    }

    public <T> T getProperty( String key, Class<T> type )
    {
        return type.cast( mProperties.get( key ) );
    }

    public void setProperty( String key, Object value )
    {
        mProperties.put( key, value );
    }

    public String getLabel()
    {
        return mLabel;
    }

    public void setLabel( String label )
    {
        mLabel = label;
    }
}
