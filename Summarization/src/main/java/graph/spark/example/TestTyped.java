package graph.spark.example;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.JsonNode;

import graph.spark.example.model.typed.AllocatedTo;
import graph.spark.example.model.typed.BasedOn;
import graph.spark.example.model.typed.Client;
import graph.spark.example.model.typed.Contains;
import graph.spark.example.model.typed.CreatedBy;
import graph.spark.example.model.typed.CreatedFor;
import graph.spark.example.model.typed.Customer;
import graph.spark.example.model.typed.DeliveryNote;
import graph.spark.example.model.typed.Edge;
import graph.spark.example.model.typed.Employee;
import graph.spark.example.model.typed.Logistics;
import graph.spark.example.model.typed.OpenedBy;
import graph.spark.example.model.typed.OperatedBy;
import graph.spark.example.model.typed.PartOf;
import graph.spark.example.model.typed.PlacedAt;
import graph.spark.example.model.typed.ProcessedBy;
import graph.spark.example.model.typed.Product;
import graph.spark.example.model.typed.PurchInvoice;
import graph.spark.example.model.typed.PurchOrder;
import graph.spark.example.model.typed.PurchOrderLine;
import graph.spark.example.model.typed.ReceivedFrom;
import graph.spark.example.model.typed.SalesInvoice;
import graph.spark.example.model.typed.SalesOrder;
import graph.spark.example.model.typed.SalesOrderLine;
import graph.spark.example.model.typed.SalesQuotation;
import graph.spark.example.model.typed.SalesQuotationLine;
import graph.spark.example.model.typed.SentBy;
import graph.spark.example.model.typed.SentTo;
import graph.spark.example.model.typed.Serves;
import graph.spark.example.model.typed.Ticket;
import graph.spark.example.model.typed.User;
import graph.spark.example.model.typed.Vendor;
import graph.spark.example.model.typed.Vertex;
import graph.spark.parser.SparkParser;

public class TestTyped
{
    public static void main( String[] args )
    {
        Logger.getLogger( "org" ).setLevel( Level.WARN );
        Logger.getLogger( "akka" ).setLevel( Level.WARN );

        System.setProperty( "spark.ui.showConsoleProgress", "false" );

        try
        {
            SparkConf        conf     = new SparkConf().setAppName( "Spark Test" ).setMaster( "local[1]" );
            JavaSparkContext context  = new JavaSparkContext( conf );
            JavaRDD<Vertex>  vertices = SparkParser.parseJson( context.textFile( "data/nodes.json" ), new VertexMapper() );
            JavaRDD<Edge>    edges    = SparkParser.parseJson( context.textFile( "data/edges.json" ), new EdgeMapper() );

            vertices.cache();
            edges.cache();

            System.out.println( "vertices: " + vertices.count() );
            System.out.println( "edges: " + edges.count() );

            System.out.println( "first Customer.name: " +
                                ( ( Customer ) vertices.filter( vertex -> vertex instanceof Customer ).collect().get( 0 ) ).name );
            System.out.println( "first PurchOrderLine.quantity: " +
                                ( ( PurchOrderLine ) edges.filter( edge -> edge instanceof PurchOrderLine ).collect().get( 0 ) ).quantity );

            context.close();
        }
        catch ( Exception e )
        {
            System.out.println( e.toString() );
        }
    }

    private static class VertexMapper implements Function<JsonNode, Vertex>
    {
        private static final long serialVersionUID = 1L;

        @Override
        public Vertex call( JsonNode node )
        {
            switch ( node.get( "meta" ).get( "label" ).asText() )
            {
                case "Client":
                {
                    return new Client( node.get( "id" ).asInt(),
                                       node.get( "data" ).get( "account" ).asText(),
                                       node.get( "data" ).get( "name" ).asText(),
                                       node.get( "data" ).get( "contactPhone" ).asText(),
                                       node.get( "data" ).get( "erpCustNum" ).asText() );
                }
                case "Customer":
                {
                    return new Customer( node.get( "id" ).asInt(),
                                         node.get( "data" ).get( "num" ).asText(),
                                         node.get( "data" ).get( "name" ).asText() );
                }
                case "DeliveryNote":
                {
                    return new DeliveryNote( node.get( "id" ).asInt(),
                                             node.get( "data" ).get( "num" ).asText(),
                                             LocalDate.parse( node.get( "data" ).get( "date" ).asText(), DateTimeFormatter.ISO_LOCAL_DATE ),
                                             node.get( "data" ).get( "trackingCode" ).asText() );
                }
                case "Employee":
                {
                    return new Employee( node.get( "id" ).asInt(),
                                         node.get( "data" ).get( "num" ).asText(),
                                         node.get( "data" ).get( "name" ).asText(),
                                         node.get( "data" ).get( "gender" ).asText() );
                }
                case "Logistics":
                {
                    return new Logistics( node.get( "id" ).asInt(),
                                          node.get( "data" ).get( "num" ).asText(),
                                          node.get( "data" ).get( "name" ).asText() );
                }
                case "Product":
                {
                    return new Product( node.get( "id" ).asInt(),
                                        node.get( "data" ).get( "num" ).asText(),
                                        node.get( "data" ).get( "name" ).asText(),
                                        node.get( "data" ).get( "category" ).asText(),
                                        new BigDecimal( node.get( "data" ).get( "price" ).asDouble() ) );
                }
                case "PurchInvoice":
                {
                    return new PurchInvoice( node.get( "id" ).asInt(),
                                             node.get( "data" ).get( "num" ).asText(),
                                             LocalDate.parse( node.get( "data" ).get( "date" ).asText(), DateTimeFormatter.ISO_LOCAL_DATE ),
                                             new BigDecimal( node.get( "data" ).get( "expense" ).asDouble() ),
                                             node.get( "data" ).get( "text" ).asText() );
                }
                case "PurchOrder":
                {
                    return new PurchOrder( node.get( "id" ).asInt(),
                                           node.get( "data" ).get( "num" ).asText(),
                                           LocalDate.parse( node.get( "data" ).get( "date" ).asText(), DateTimeFormatter.ISO_LOCAL_DATE ) );
                }
                case "SalesInvoice":
                {
                    return new SalesInvoice( node.get( "id" ).asInt(),
                                             node.get( "data" ).get( "num" ).asText(),
                                             LocalDate.parse( node.get( "data" ).get( "date" ).asText(), DateTimeFormatter.ISO_LOCAL_DATE ),
                                             new BigDecimal( node.get( "data" ).get( "revenue" ).asDouble() ),
                                             node.get( "data" ).get( "text" ).asText() );
                }
                case "SalesOrder":
                {
                    return new SalesOrder( node.get( "id" ).asInt(),
                                           node.get( "data" ).get( "num" ).asText(),
                                           LocalDate.parse( node.get( "data" ).get( "date" ).asText(), DateTimeFormatter.ISO_LOCAL_DATE ),
                                           LocalDate.parse( node.get( "data" ).get( "deliveryDate" ).asText(), DateTimeFormatter.ISO_LOCAL_DATE ) );
                }
                case "SalesQuotation":
                {
                    return new SalesQuotation( node.get( "id" ).asInt(),
                                               node.get( "data" ).get( "num" ).asText(),
                                               LocalDate.parse( node.get( "data" ).get( "date" ).asText(), DateTimeFormatter.ISO_LOCAL_DATE ) );
                }
                case "Ticket":
                {
                    return new Ticket( node.get( "id" ).asInt(),
                                       node.get( "data" ).get( "id" ).asInt(),
                                       LocalDate.parse( node.get( "data" ).get( "createdAt" ).asText(), DateTimeFormatter.ISO_LOCAL_DATE ),
                                       node.get( "data" ).get( "problem" ).asText(),
                                       node.get( "data" ).get( "erpSoNum" ).asText() );
                }
                case "User":
                {
                    return new User( node.get( "id" ).asInt(),
                                     node.get( "data" ).get( "email" ).asText(),
                                     node.get( "data" ).get( "name" ).asText(),
                                     node.get( "data" ).get( "erpEmplNum" ).asText() );
                }
                case "Vendor":
                {
                    return new Vendor( node.get( "id" ).asInt(),
                                       node.get( "data" ).get( "num" ).asText(),
                                       node.get( "data" ).get( "name" ).asText() );
                }
                default:
                {
                    throw new RuntimeException( "Oh dears..." );
                }
            }
        }
    }

    private static class EdgeMapper implements Function<JsonNode, Edge>
    {
        private static final long serialVersionUID = 1L;

        @Override
        public Edge call( JsonNode node )
        {
            switch ( node.get( "meta" ).get( "label" ).asText() )
            {
                case "allocatedTo":
                {
                    return new AllocatedTo( node.get( "source" ).asInt(),
                                            node.get( "target" ).asInt() );
                }
                case "basedOn":
                {
                    return new BasedOn( node.get( "source" ).asInt(),
                                        node.get( "target" ).asInt() );
                }
                case "contains":
                {
                    return new Contains( node.get( "source" ).asInt(),
                                         node.get( "target" ).asInt() );
                }
                case "createdBy":
                {
                    return new CreatedBy( node.get( "source" ).asInt(),
                                          node.get( "target" ).asInt() );
                }
                case "createdFor":
                {
                    return new CreatedFor( node.get( "source" ).asInt(),
                                           node.get( "target" ).asInt() );
                }
                case "openedBy":
                {
                    return new OpenedBy( node.get( "source" ).asInt(),
                                         node.get( "target" ).asInt() );
                }
                case "operatedBy":
                {
                    return new OperatedBy( node.get( "source" ).asInt(),
                                           node.get( "target" ).asInt() );
                }
                case "partOf":
                {
                    return new PartOf( node.get( "source" ).asInt(),
                                       node.get( "target" ).asInt() );
                }
                case "placedAt":
                {
                    return new PlacedAt( node.get( "source" ).asInt(),
                                         node.get( "target" ).asInt() );
                }
                case "processedBy":
                {
                    return new ProcessedBy( node.get( "source" ).asInt(),
                                            node.get( "target" ).asInt() );
                }
                case "PurchOrderLine":
                {
                    return new PurchOrderLine( node.get( "source" ).asInt(),
                                               node.get( "target" ).asInt(),
                                               node.get( "data" ).get( "quantity" ).asInt(),
                                               new BigDecimal( node.get( "data" ).get( "purchPrice" ).asDouble() ) );
                }
                case "receivedFrom":
                {
                    return new ReceivedFrom( node.get( "source" ).asInt(),
                                             node.get( "target" ).asInt() );
                }
                case "SalesOrderLine":
                {
                    return new SalesOrderLine( node.get( "source" ).asInt(),
                                               node.get( "target" ).asInt(),
                                               node.get( "data" ).get( "quantity" ).asInt(),
                                               new BigDecimal( node.get( "data" ).get( "salesPrice" ).asDouble() ) );
                }
                case "SalesQuotationLine":
                {
                    return new SalesQuotationLine( node.get( "source" ).asInt(),
                                                   node.get( "target" ).asInt(),
                                                   node.get( "data" ).get( "quantity" ).asInt(),
                                                   new BigDecimal( node.get( "data" ).get( "salesPrice" ).asDouble() ),
                                                   new BigDecimal( node.get( "data" ).get( "purchPrice" ).asDouble() ) );
                }
                case "sentBy":
                {
                    return new SentBy( node.get( "source" ).asInt(),
                                       node.get( "target" ).asInt() );
                }
                case "sentTo":
                {
                    return new SentTo( node.get( "source" ).asInt(),
                                       node.get( "target" ).asInt() );
                }
                case "serves":
                {
                    return new Serves( node.get( "source" ).asInt(),
                                       node.get( "target" ).asInt() );
                }
                default:
                {
                    throw new RuntimeException( "Oh dears..." );
                }
            }
        }
    }
}
