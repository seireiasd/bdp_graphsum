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
import graph.spark.example.model.generic.GenericAttributes;
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
            SparkConf                          conf     = new SparkConf().setAppName( "Spark Test" ).setMaster( "local[1]" );
            JavaSparkContext                   context  = new JavaSparkContext( conf );
            JavaRDD<Vertex<GenericAttributes>> vertices = SparkParser.parseJson( context.textFile( "data/nodes.json" ), new VertexMapper() );
            JavaRDD<Edge<GenericAttributes>>   edges    = SparkParser.parseJson( context.textFile( "data/edges.json" ), new EdgeMapper() );

            vertices.cache();
            edges.cache();

            System.out.println( "vertices: " + vertices.count() );
            System.out.println( "edges: " + edges.count() );

            System.out.println( "first Customer.name: " +
                                vertices.filter( vertex -> vertex.getData().pairs.get( "label" ).equals( "Customer" ) )
                                        .collect().get( 0 ).getData().pairs.get( "name" ) );
            System.out.println( "first PurchOrderLine.quantity: " +
                                edges.filter( edge -> edge.getData().pairs.get( "label" ).equals( "PurchOrderLine" ) )
                                     .collect().get( 0 ).getData().pairs.get( "quantity" ) );

            context.close();
        }
        catch ( Exception e )
        {
            System.out.println( e.toString() );
        }
    }

    private static class VertexMapper implements Function<JsonNode, Vertex<GenericAttributes>>
    {
        private static final long serialVersionUID = 1L;

        @Override
        public Vertex<GenericAttributes> call( JsonNode node )
        {
            GenericAttributes attributes = new GenericAttributes();
            String            label      = node.get( "meta" ).get( "label" ).asText();

            attributes.pairs.put( "label", label );

            switch ( label )
            {
                case "Client":
                {
                    attributes.pairs.put( "account", node.get( "data" ).get( "account" ).asText() );
                    attributes.pairs.put( "name", node.get( "data" ).get( "name" ).asText() );
                    attributes.pairs.put( "contactPhone", node.get( "data" ).get( "contactPhone" ).asText() );
                    attributes.pairs.put( "erpCustNum", node.get( "data" ).get( "erpCustNum" ).asText() );

                    break;
                }
                case "Customer":
                {
                    attributes.pairs.put( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.pairs.put( "name", node.get( "data" ).get( "name" ).asText() );

                    break;
                }
                case "DeliveryNote":
                {
                    attributes.pairs.put( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.pairs.put( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                   DateTimeFormatter.ISO_LOCAL_DATE ) );
                    attributes.pairs.put( "trackingCode", node.get( "data" ).get( "trackingCode" ).asText() );

                    break;
                }
                case "Employee":
                {
                    attributes.pairs.put( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.pairs.put( "name", node.get( "data" ).get( "name" ).asText() );
                    attributes.pairs.put( "gender", node.get( "data" ).get( "gender" ).asText() );

                    break;
                }
                case "Logistics":
                {
                    attributes.pairs.put( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.pairs.put( "name", node.get( "data" ).get( "name" ).asText() );

                    break;
                }
                case "Product":
                {
                    attributes.pairs.put( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.pairs.put( "name", node.get( "data" ).get( "name" ).asText() );
                    attributes.pairs.put( "category", node.get( "data" ).get( "category" ).asText() );
                    attributes.pairs.put( "price", new BigDecimal( node.get( "data" ).get( "price" ).asDouble() ) );

                    break;
                }
                case "PurchInvoice":
                {
                    attributes.pairs.put( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.pairs.put( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                   DateTimeFormatter.ISO_LOCAL_DATE ) );
                    attributes.pairs.put( "expense", new BigDecimal( node.get( "data" ).get( "expense" ).asDouble() ) );
                    attributes.pairs.put( "text", node.get( "data" ).get( "text" ).asText() );

                    break;
                }
                case "PurchOrder":
                {
                    attributes.pairs.put( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.pairs.put( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                   DateTimeFormatter.ISO_LOCAL_DATE ) );

                    break;
                }
                case "SalesInvoice":
                {
                    attributes.pairs.put( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.pairs.put( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                   DateTimeFormatter.ISO_LOCAL_DATE ) );
                    attributes.pairs.put( "revenue", new BigDecimal( node.get( "data" ).get( "revenue" ).asDouble() ) );
                    attributes.pairs.put( "text", node.get( "data" ).get( "text" ).asText() );

                    break;
                }
                case "SalesOrder":
                {
                    attributes.pairs.put( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.pairs.put( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                   DateTimeFormatter.ISO_LOCAL_DATE ) );
                    attributes.pairs.put( "deliveryDate", LocalDate.parse( node.get( "data" ).get( "deliveryDate" ).asText(),
                                                                           DateTimeFormatter.ISO_LOCAL_DATE ) );

                    break;
                }
                case "SalesQuotation":
                {
                    attributes.pairs.put( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.pairs.put( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                   DateTimeFormatter.ISO_LOCAL_DATE ) );

                    break;
                }
                case "Ticket":
                {
                    attributes.pairs.put( "id", node.get( "data" ).get( "id" ).asInt() );
                    attributes.pairs.put( "createdAt", LocalDate.parse( node.get( "data" ).get( "createdAt" ).asText(),
                                                                        DateTimeFormatter.ISO_LOCAL_DATE ) );
                    attributes.pairs.put( "problem", node.get( "data" ).get( "problem" ).asText() );
                    attributes.pairs.put( "erpSoNum", node.get( "data" ).get( "erpSoNum" ).asText() );

                    break;
                }
                case "User":
                {
                    attributes.pairs.put( "email", node.get( "data" ).get( "email" ).asText() );
                    attributes.pairs.put( "name", node.get( "data" ).get( "name" ).asText() );
                    attributes.pairs.put( "erpEmplNum", node.get( "data" ).get( "erpEmplNum" ).asText() );

                    break;
                }
                case "Vendor":
                {
                    attributes.pairs.put( "num", node.get( "data" ).get( "num" ).asText() );
                    attributes.pairs.put( "name", node.get( "data" ).get( "name" ).asText() );

                    break;
                }
                default:
                {
                    throw new RuntimeException( "Oh dears..." );
                }
            }

            return new Vertex<GenericAttributes>( node.get( "id" ).asLong(), attributes );
        }
    }

    private static class EdgeMapper implements Function<JsonNode, Edge<GenericAttributes>>
    {
        private static final long serialVersionUID = 1L;

        @Override
        public Edge<GenericAttributes> call( JsonNode node )
        {
            GenericAttributes attributes = new GenericAttributes();
            String            label      = node.get( "meta" ).get( "label" ).asText();

            attributes.pairs.put( "label", label );

            switch ( label )
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
                    attributes.pairs.put( "quantity", node.get( "data" ).get( "quantity" ).asInt() );
                    attributes.pairs.put( "purchPrice", new BigDecimal( node.get( "data" ).get( "purchPrice" ).asDouble() ) );

                    break;
                }
                case "SalesOrderLine":
                {
                    attributes.pairs.put( "quantity", node.get( "data" ).get( "quantity" ).asInt() );
                    attributes.pairs.put( "salesPrice", new BigDecimal( node.get( "data" ).get( "salesPrice" ).asDouble() ) );

                    break;
                }
                case "SalesQuotationLine":
                {
                    attributes.pairs.put( "quantity", node.get( "data" ).get( "quantity" ).asInt() );
                    attributes.pairs.put( "salesPrice", new BigDecimal( node.get( "data" ).get( "salesPrice" ).asDouble() ) );
                    attributes.pairs.put( "purchPrice", new BigDecimal( node.get( "data" ).get( "purchPrice" ).asDouble() ) );

                    break;
                }
                default:
                {
                    throw new RuntimeException( "Oh dears..." );
                }
            }

            return new Edge<GenericAttributes>( node.get( "source" ).asLong(), node.get( "target" ).asLong(), attributes );
        }
    }
}
