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
import graph.spark.example.model.generic.Attributes;
import graph.spark.parser.SparkParser;

public class TestGeneric
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
                                vertices.filter( vertex -> vertex.getData().getLabel().equals( "Customer" ) )
                                        .collect().get( 0 ).getData().getProperty( "name" ) );
            System.out.println( "first PurchOrderLine.quantity: " +
                                edges.filter( edge -> edge.getData().getLabel().equals( "PurchOrderLine" ) )
                                     .collect().get( 0 ).getData().getProperty( "quantity" ) );

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
            Attributes attributes = new Attributes( node.get( "meta" ).get( "label" ).asText() );

            switch ( attributes.getLabel() )
            {
                case "Client":
                {
                    attributes.setProperty( "account", node.get( "data" ).get( "account" ).asText() );
                    attributes.setProperty( "name", node.get( "data" ).get( "name" ).asText() );
                    attributes.setProperty( "contactPhone", node.get( "data" ).get( "contactPhone" ).asText() );
                    attributes.setProperty( "erpCustNum", node.get( "data" ).get( "erpCustNum" ).asText() );

                    break;
                }
                case "Customer":
                {
                    attributes.setProperty( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.setProperty( "name", node.get( "data" ).get( "name" ).asText() );

                    break;
                }
                case "DeliveryNote":
                {
                    attributes.setProperty( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.setProperty( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                     DateTimeFormatter.ISO_LOCAL_DATE ) );
                    attributes.setProperty( "trackingCode", node.get( "data" ).get( "trackingCode" ).asText() );

                    break;
                }
                case "Employee":
                {
                    attributes.setProperty( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.setProperty( "name", node.get( "data" ).get( "name" ).asText() );
                    attributes.setProperty( "gender", node.get( "data" ).get( "gender" ).asText() );

                    break;
                }
                case "Logistics":
                {
                    attributes.setProperty( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.setProperty( "name", node.get( "data" ).get( "name" ).asText() );

                    break;
                }
                case "Product":
                {
                    attributes.setProperty( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.setProperty( "name", node.get( "data" ).get( "name" ).asText() );
                    attributes.setProperty( "category", node.get( "data" ).get( "category" ).asText() );
                    attributes.setProperty( "price", new BigDecimal( node.get( "data" ).get( "price" ).asDouble() ) );

                    break;
                }
                case "PurchInvoice":
                {
                    attributes.setProperty( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.setProperty( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                     DateTimeFormatter.ISO_LOCAL_DATE ) );
                    attributes.setProperty( "expense", new BigDecimal( node.get( "data" ).get( "expense" ).asDouble() ) );
                    attributes.setProperty( "text", node.get( "data" ).get( "text" ).asText() );

                    break;
                }
                case "PurchOrder":
                {
                    attributes.setProperty( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.setProperty( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                     DateTimeFormatter.ISO_LOCAL_DATE ) );

                    break;
                }
                case "SalesInvoice":
                {
                    attributes.setProperty( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.setProperty( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                     DateTimeFormatter.ISO_LOCAL_DATE ) );
                    attributes.setProperty( "revenue", new BigDecimal( node.get( "data" ).get( "revenue" ).asDouble() ) );
                    attributes.setProperty( "text", node.get( "data" ).get( "text" ).asText() );

                    break;
                }
                case "SalesOrder":
                {
                    attributes.setProperty( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.setProperty( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                     DateTimeFormatter.ISO_LOCAL_DATE ) );
                    attributes.setProperty( "deliveryDate", LocalDate.parse( node.get( "data" ).get( "deliveryDate" ).asText(),
                                                                             DateTimeFormatter.ISO_LOCAL_DATE ) );

                    break;
                }
                case "SalesQuotation":
                {
                    attributes.setProperty( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.setProperty( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                     DateTimeFormatter.ISO_LOCAL_DATE ) );

                    break;
                }
                case "Ticket":
                {
                    attributes.setProperty( "id", node.get( "data" ).get( "id" ).asInt() );
                    attributes.setProperty( "createdAt", LocalDate.parse( node.get( "data" ).get( "createdAt" ).asText(),
                                                                          DateTimeFormatter.ISO_LOCAL_DATE ) );
                    attributes.setProperty( "problem", node.get( "data" ).get( "problem" ).asText() );
                    attributes.setProperty( "erpSoNum", node.get( "data" ).get( "erpSoNum" ).asText() );

                    break;
                }
                case "User":
                {
                    attributes.setProperty( "email", node.get( "data" ).get( "email" ).asText() );
                    attributes.setProperty( "name", node.get( "data" ).get( "name" ).asText() );
                    attributes.setProperty( "erpEmplNum", node.get( "data" ).get( "erpEmplNum" ).asText() );

                    break;
                }
                case "Vendor":
                {
                    attributes.setProperty( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.setProperty( "name", node.get( "data" ).get( "name" ).asText() );

                    break;
                }
                default:
                {
                    throw new RuntimeException( "Oh dears..." );
                }
            }

            return new Vertex<Attributes>( node.get( "id" ).asLong(), attributes );
        }
    }

    private static class EdgeMapper implements Function<JsonNode, Edge<Attributes>>
    {
        private static final long serialVersionUID = 1L;

        @Override
        public Edge<Attributes> call( JsonNode node )
        {
            Attributes attributes = new Attributes( node.get( "meta" ).get( "label" ).asText() );

            switch ( attributes.getLabel() )
            {
                case "allocatedTo":
                case "basedOn":
                case "contains":
                case "createdBy":
                case "createdFor":
                case "openedBy":
                case "operatedBy":
                case "partOf":
                case "placedAt":
                case "processedBy":
                case "receivedFrom":
                case "sentBy":
                case "sentTo":
                case "serves":
                {
                    break;
                }
                case "PurchOrderLine":
                {
                    attributes.setProperty( "quantity", node.get( "data" ).get( "quantity" ).asInt() );
                    attributes.setProperty( "purchPrice", new BigDecimal( node.get( "data" ).get( "purchPrice" ).asDouble() ) );

                    break;
                }
                case "SalesOrderLine":
                {
                    attributes.setProperty( "quantity", node.get( "data" ).get( "quantity" ).asInt() );
                    attributes.setProperty( "salesPrice", new BigDecimal( node.get( "data" ).get( "salesPrice" ).asDouble() ) );

                    break;
                }
                case "SalesQuotationLine":
                {
                    attributes.setProperty( "quantity", node.get( "data" ).get( "quantity" ).asInt() );
                    attributes.setProperty( "salesPrice", new BigDecimal( node.get( "data" ).get( "salesPrice" ).asDouble() ) );
                    attributes.setProperty( "purchPrice", new BigDecimal( node.get( "data" ).get( "purchPrice" ).asDouble() ) );

                    break;
                }
                default:
                {
                    throw new RuntimeException( "Oh dears..." );
                }
            }

            return new Edge<Attributes>( node.get( "source" ).asLong(), node.get( "target" ).asLong(), attributes );
        }
    }
}
