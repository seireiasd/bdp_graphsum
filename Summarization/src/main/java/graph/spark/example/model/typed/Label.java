package graph.spark.example.model.typed;

public class Label implements Attributes
{
    private static final long serialVersionUID = 1L;

    public static final String allocatedTo  = "allocatedTo";
    public static final String basedOn      = "basedOn";
    public static final String contains     = "contains";
    public static final String createdBy    = "createdBy";
    public static final String createdFor   = "createdFor";
    public static final String openedBy     = "openedBy";
    public static final String operatedBy   = "operatedBy";
    public static final String partOf       = "partOf";
    public static final String placedAt     = "placedAt";
    public static final String processedBy  = "processedBy";
    public static final String receivedFrom = "receivedFrom";
    public static final String sentBy       = "sentBy";
    public static final String sentTo       = "sentTo";
    public static final String serves       = "serves";

    private String mLabel;

    public Label( String label )
    {
        mLabel = label;
    }

    @Override
    public String getLabel()
    {
        return mLabel;
    }
}
