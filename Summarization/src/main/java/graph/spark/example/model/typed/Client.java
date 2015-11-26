package graph.spark.example.model.typed;

public class Client implements Attributes
{
    private static final long serialVersionUID = 1L;

    public static final String label = "Client";

    public final String account;
    public final String name;
    public final String contactPhone;
    public final String erpCustNum;

    public Client( String account, String name, String contactPhone, String erpCustNum )
    {
        this.account      = account;
        this.name         = name;
        this.contactPhone = contactPhone;
        this.erpCustNum   = erpCustNum;
    }

    @Override
    public String getLabel()
    {
        return label;
    }
}
