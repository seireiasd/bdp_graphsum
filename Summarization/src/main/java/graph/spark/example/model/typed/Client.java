package graph.spark.example.model.typed;

public class Client extends Vertex
{
    private static final long serialVersionUID = 1L;

    public final String account;
    public final String name;
    public final String contactPhone;
    public final String erpCustNum;

    public Client( int nodeId, String account, String name, String contactPhone, String erpCustNum )
    {
        super( nodeId );

        this.account      = account;
        this.name         = name;
        this.contactPhone = contactPhone;
        this.erpCustNum   = erpCustNum;
    }
}
