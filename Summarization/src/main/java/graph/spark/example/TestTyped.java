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

import graph.spark.Edge;
import graph.spark.Vertex;
import graph.spark.example.model.typed.Attributes;
import graph.spark.example.model.typed.Client;
import graph.spark.example.model.typed.Customer;
import graph.spark.example.model.typed.DeliveryNote;
import graph.spark.example.model.typed.Employee;
import graph.spark.example.model.typed.Label;
import graph.spark.example.model.typed.Logistics;
import graph.spark.example.model.typed.Product;
import graph.spark.example.model.typed.PurchInvoice;
import graph.spark.example.model.typed.PurchOrder;
import graph.spark.example.model.typed.PurchOrderLine;
import graph.spark.example.model.typed.SalesInvoice;
import graph.spark.example.model.typed.SalesOrder;
import graph.spark.example.model.typed.SalesOrderLine;
import graph.spark.example.model.typed.SalesQuotation;
import graph.spark.example.model.typed.SalesQuotationLine;
import graph.spark.example.model.typed.Ticket;
import graph.spark.example.model.typed.User;
import graph.spark.example.model.typed.Vendor;
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
            SparkConf                   conf     = new SparkConf().setAppName( "Spark Test" ).setMaster( "local[1]" );
            JavaSparkContext            context  = new JavaSparkContext( conf );
            JavaRDD<Vertex<Attributes>> vertices = SparkParser.parseJson( context.textFile( "data/nodes.json" ), new VertexMapper() );
            JavaRDD<Edge<Attributes>>   edges    = SparkParser.parseJson( context.textFile( "data/edges.json" ), new EdgeMapper() );

            vertices.cache();
            edges.cache();

            System.out.println( "vertices: " + vertices.count() );
            System.out.println( "edges: " + edges.count() );

            System.out.println( "first Customer.name: " +
                                ( ( Customer ) vertices.filter( vertex -> vertex.getData().getLabel() == Customer.label )
                                                       .collect().get( 0 ).getData() ).name );
            System.out.println( "first PurchOrderLine.quantity: " +
                                ( ( PurchOrderLine ) edges.filter( edge -> edge.getData().getLabel() == PurchOrderLine.label )
                                                          .collect().get( 0 ).getData() ).quantity );

            context.close();
        }
        catch ( Exception e )
        {
            System.out.println( e.toString() );
        }
    }

    private static class VertexMapper implements Function<JsonNode, Vertex<Attributes>>
    {
        private static final long serialVersionUID = 1L;

        @Override
        public Vertex<Attributes> call( JsonNode node )
        {
            switch ( node.get( "meta" ).get( "label" ).asText() )
            {
                case Client.label:
                {
                    return new Vertex<Attributes>( node.get( "id" ).asLong(),
                                                   new Client( node.get( "data" ).get( "account" ).asText(),
                                                               node.get( "data" ).get( "name" ).asText(),
                                                               node.get( "data" ).get( "contactPhone" ).asText(),
                                                               node.get( "data" ).get( "erpCustNum" ).asText() ) );
                }
                case Customer.label:
                {
                    return new Vertex<Attributes>( node.get( "id" ).asLong(),
                                                   new Customer( node.get( "data" ).get( "num" ).asText(),
                                                                 node.get( "data" ).get( "name" ).asText() ) );
                }
                case DeliveryNote.label:
                {
                    return new Vertex<Attributes>( node.get( "id" ).asLong(),
                                                   new DeliveryNote( node.get( "data" ).get( "num" ).asText(),
                                                                     LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                                      DateTimeFormatter.ISO_LOCAL_DATE ),
                                                                     node.get( "data" ).get( "trackingCode" ).asText() ) );
                }
                case Employee.label:
                {
                    return new Vertex<Attributes>( node.get( "id" ).asLong(),
                                                   new Employee( node.get( "data" ).get( "num" ).asText(),
                                                                 node.get( "data" ).get( "name" ).asText(),
                                                                 node.get( "data" ).get( "gender" ).asText() ) );
                }
                case Logistics.label:
                {
                    return new Vertex<Attributes>( node.get( "id" ).asLong(),
                                                   new Logistics( node.get( "data" ).get( "num" ).asText(),
                                                                  node.get( "data" ).get( "name" ).asText() ) );
                }
                case Product.label:
                {
                    return new Vertex<Attributes>( node.get( "id" ).asLong(),
                                                   new Product( node.get( "data" ).get( "num" ).asText(),
                                                                node.get( "data" ).get( "name" ).asText(),
                                                                node.get( "data" ).get( "category" ).asText(),
                                                                new BigDecimal( node.get( "data" ).get( "price" ).asDouble() ) ) );
                }
                case PurchInvoice.label:
                {
                    return new Vertex<Attributes>( node.get( "id" ).asLong(),
                                                   new PurchInvoice( node.get( "data" ).get( "num" ).asText(),
                                                                     LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                                      DateTimeFormatter.ISO_LOCAL_DATE ),
                                                                     new BigDecimal( node.get( "data" ).get( "expense" ).asDouble() ),
                                                                     node.get( "data" ).get( "text" ).asText() ) );
                }
                case PurchOrder.label:
                {
                    return new Vertex<Attributes>( node.get( "id" ).asLong(),
                                                   new PurchOrder( node.get( "data" ).get( "num" ).asText(),
                                                                   LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                                    DateTimeFormatter.ISO_LOCAL_DATE ) ) );
                }
                case SalesInvoice.label:
                {
                    return new Vertex<Attributes>( node.get( "id" ).asLong(),
                                                   new SalesInvoice( node.get( "data" ).get( "num" ).asText(),
                                                                     LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                                      DateTimeFormatter.ISO_LOCAL_DATE ),
                                                                     new BigDecimal( node.get( "data" ).get( "revenue" ).asDouble() ),
                                                                     node.get( "data" ).get( "text" ).asText() ) );
                }
                case SalesOrder.label:
                {
                    return new Vertex<Attributes>( node.get( "id" ).asLong(),
                                                   new SalesOrder( node.get( "data" ).get( "num" ).asText(),
                                                                   LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                                    DateTimeFormatter.ISO_LOCAL_DATE ),
                                                                   LocalDate.parse( node.get( "data" ).get( "deliveryDate" ).asText(),
                                                                                    DateTimeFormatter.ISO_LOCAL_DATE ) ) );
                }
                case SalesQuotation.label:
                {
                    return new Vertex<Attributes>( node.get( "id" ).asLong(),
                                                   new SalesQuotation( node.get( "data" ).get( "num" ).asText(),
                                                                       LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                                        DateTimeFormatter.ISO_LOCAL_DATE ) ) );
                }
                case Ticket.label:
                {
                    return new Vertex<Attributes>( node.get( "id" ).asLong(),
                                                   new Ticket( node.get( "data" ).get( "id" ).asInt(),
                                                               LocalDate.parse( node.get( "data" ).get( "createdAt" ).asText(),
                                                                                DateTimeFormatter.ISO_LOCAL_DATE ),
                                                               node.get( "data" ).get( "problem" ).asText(),
                                                               node.get( "data" ).get( "erpSoNum" ).asText() ) );
                }
                case User.label:
                {
                    return new Vertex<Attributes>( node.get( "id" ).asLong(),
                                                   new User( node.get( "data" ).get( "email" ).asText(),
                                                             node.get( "data" ).get( "name" ).asText(),
                                                             node.get( "data" ).get( "erpEmplNum" ).asText() ) );
                }
                case Vendor.label:
                {
                    return new Vertex<Attributes>( node.get( "id" ).asLong(),
                                                   new Vendor( node.get( "data" ).get( "num" ).asText(),
                                                               node.get( "data" ).get( "name" ).asText() ) );
                }
                default:
                {
                    throw new RuntimeException( "Oh dears..." );
                }
            }
        }
    }

    private static class EdgeMapper implements Function<JsonNode, Edge<Attributes>>
    {
        private static final long serialVersionUID = 1L;

        @Override
        public Edge<Attributes> call( JsonNode node )
        {
            switch ( node.get( "meta" ).get( "label" ).asText() )
            {
                case Label.allocatedTo:
                case Label.basedOn:
                case Label.contains:
                case Label.createdBy:
                case Label.createdFor:
                case Label.openedBy:
                case Label.operatedBy:
                case Label.partOf:
                case Label.placedAt:
                case Label.processedBy:
                case Label.receivedFrom:
                case Label.sentBy:
                case Label.sentTo:
                case Label.serves:
                {
                    return new Edge<Attributes>( node.get( "source" ).asLong(),
                                                 node.get( "target" ).asLong(),
                                                 new Label( node.get( "meta" ).get( "label" ).asText() ) );
                }
                case PurchOrderLine.label:
                {
                    return new Edge<Attributes>( node.get( "source" ).asLong(),
                                                 node.get( "target" ).asLong(),
                                                 new PurchOrderLine( node.get( "data" ).get( "quantity" ).asInt(),
                                                                     new BigDecimal( node.get( "data" ).get( "purchPrice" ).asDouble() ) ) );
                }
                case SalesOrderLine.label:
                {
                    return new Edge<Attributes>( node.get( "source" ).asLong(),
                                                 node.get( "target" ).asLong(),
                                                 new SalesOrderLine( node.get( "data" ).get( "quantity" ).asInt(),
                                                                     new BigDecimal( node.get( "data" ).get( "salesPrice" ).asDouble() ) ) );
                }
                case SalesQuotationLine.label:
                {
                    return new Edge<Attributes>( node.get( "source" ).asLong(),
                                                 node.get( "target" ).asLong(),
                                                 new SalesQuotationLine( node.get( "data" ).get( "quantity" ).asInt(),
                                                                         new BigDecimal( node.get( "data" ).get( "salesPrice" ).asDouble() ),
                                                                         new BigDecimal( node.get( "data" ).get( "purchPrice" ).asDouble() ) ) );
                }
                default:
                {
                    throw new RuntimeException( "Oh dears..." );
                }
            }
        }
    }
}
