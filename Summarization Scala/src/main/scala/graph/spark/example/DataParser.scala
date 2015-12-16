package graph.spark.example

import java.math.BigDecimal
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId

import com.fasterxml.jackson.databind.JsonNode

import graph.spark.parser.SparkParser

object DataParser
{
    def parseGraph( vertexPath: String, edgePath: String, context: SparkContext ): Graph[Attributes, Attributes] =
    {
        val vertices = SparkParser.parseJson( context.textFile( vertexPath ), new VertexMapper() )
        val edges    = SparkParser.parseJson( context.textFile( edgePath ), new EdgeMapper() );

        Graph( vertices, edges )
    }

    private class VertexMapper extends ( JsonNode => ( VertexId, Attributes ) ) with Serializable
    {
        def apply( node: JsonNode ): ( VertexId, Attributes ) =
        {
            val label = node.get( "meta" ).get( "label" ).asText()

            val attributes = label match
            {
                case "Client" => Map( ( "account", node.get( "data" ).get( "account" ).asText() ),
                                      ( "name", node.get( "data" ).get( "name" ).asText() ),
                                      ( "contactPhone", node.get( "data" ).get( "contactPhone" ).asText() ),
                                      ( "erpCustNum", node.get( "data" ).get( "erpCustNum" ).asText() ) )

                case "Customer" => Map( ( "num", node.get( "data" ).get( "num" ).asText() ),
                                        ( "name", node.get( "data" ).get( "name" ).asText() ) )

                case "DeliveryNote" => Map( ( "num", node.get( "data" ).get( "num" ).asText() ),
                                            ( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                       DateTimeFormatter.ISO_LOCAL_DATE ) ),
                                            ( "trackingCode", node.get( "data" ).get( "trackingCode" ).asText() ) )

                case "Employee" => Map( ( "num", node.get( "data" ).get( "num" ).asText() ),
                                        ( "name", node.get( "data" ).get( "name" ).asText() ),
                                        ( "gender", node.get( "data" ).get( "gender" ).asText() ) )

                case "Logistics" => Map( ( "num", node.get( "data" ).get( "num" ).asText() ),
                                         ( "name", node.get( "data" ).get( "name" ).asText() ) )

                case "Product" => Map( ( "num", node.get( "data" ).get( "num" ).asText() ),
                                       ( "name", node.get( "data" ).get( "name" ).asText() ),
                                       ( "category", node.get( "data" ).get( "category" ).asText() ),
                                       ( "price", new BigDecimal( node.get( "data" ).get( "price" ).asDouble() ) ) )

                case "PurchInvoice" => Map( ( "num", node.get( "data" ).get( "num" ).asText() ),
                                            ( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                       DateTimeFormatter.ISO_LOCAL_DATE ) ),
                                            ( "expense", new BigDecimal( node.get( "data" ).get( "expense" ).asDouble() ) ),
                                            ( "text", node.get( "data" ).get( "text" ).asText() ) )

                case "PurchOrder" => Map( ( "num", node.get( "data" ).get( "num" ).asText() ),
                                          ( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                     DateTimeFormatter.ISO_LOCAL_DATE ) ) )

                case "SalesInvoice" => Map( ( "num", node.get( "data" ).get( "num" ).asText() ),
                                            ( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                       DateTimeFormatter.ISO_LOCAL_DATE ) ),
                                            ( "revenue", new BigDecimal( node.get( "data" ).get( "revenue" ).asDouble() ) ),
                                            ( "text", node.get( "data" ).get( "text" ).asText() ) )

                case "SalesOrder" => Map( ( "num", node.get( "data" ).get( "num" ).asText() ),
                                          ( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                     DateTimeFormatter.ISO_LOCAL_DATE ) ),
                                          ( "deliveryDate", LocalDate.parse( node.get( "data" ).get( "deliveryDate" ).asText(),
                                                                             DateTimeFormatter.ISO_LOCAL_DATE ) ) )

                case "SalesQuotation" => Map( ( "num", node.get( "data" ).get( "num" ).asText() ),
                                              ( "date", LocalDate.parse( node.get( "data" ).get( "date" ).asText(),
                                                                         DateTimeFormatter.ISO_LOCAL_DATE ) ) )

                case "Ticket" => Map( ( "id", node.get( "data" ).get( "id" ).asInt() ),
                                      ( "createdAt", LocalDate.parse( node.get( "data" ).get( "createdAt" ).asText(),
                                                                      DateTimeFormatter.ISO_LOCAL_DATE ) ),
                                      ( "problem", node.get( "data" ).get( "problem" ).asText() ),
                                      ( "erpSoNum", node.get( "data" ).get( "erpSoNum" ).asText() ) )

                case "User" => Map( ( "email", node.get( "data" ).get( "email" ).asText() ),
                                    ( "name", node.get( "data" ).get( "name" ).asText() ),
                                    ( "erpEmplNum", node.get( "data" ).get( "erpEmplNum" ).asText() ) )

                case "Vendor" => Map( ( "num", node.get( "data" ).get( "num" ).asText() ),
                                      ( "name", node.get( "data" ).get( "name" ).asText() ) )

                case _ => throw new RuntimeException( "Oh dears..." )
            }

            ( node.get( "id" ).asLong(), new Attributes( label, attributes ) )
        }
    }

    private class EdgeMapper extends ( JsonNode => Edge[Attributes] ) with Serializable
    {
        def apply( node: JsonNode ): Edge[Attributes] =
        {
            val label = node.get( "meta" ).get( "label" ).asText()

            val attributes = label match
            {
                case "allocatedTo"  |
                     "basedOn"      |
                     "contains"     |
                     "createdBy"    |
                     "createdFor"   |
                     "openedBy"     |
                     "operatedBy"   |
                     "partOf"       |
                     "placedAt"     |
                     "processedBy"  |
                     "receivedFrom" |
                     "sentBy"       |
                     "sentTo"       |
                     "serves"       => Map[String, Any]()

                case "PurchOrderLine" => Map( ( "quantity", node.get( "data" ).get( "quantity" ).asInt() ),
                                              ( "purchPrice", new BigDecimal( node.get( "data" ).get( "purchPrice" ).asDouble() ) ) )

                case "SalesOrderLine" => Map( ( "quantity", node.get( "data" ).get( "quantity" ).asInt() ),
                                              ( "salesPrice", new BigDecimal( node.get( "data" ).get( "salesPrice" ).asDouble() ) ) )

                case "SalesQuotationLine" => Map( ( "quantity", node.get( "data" ).get( "quantity" ).asInt() ),
                                                  ( "salesPrice", new BigDecimal( node.get( "data" ).get( "salesPrice" ).asDouble() ) ),
                                                  ( "purchPrice", new BigDecimal( node.get( "data" ).get( "purchPrice" ).asDouble() ) ) )

                case _ => throw new RuntimeException( "Oh dears..." )
            }

            Edge( node.get( "source" ).asLong(), node.get( "target" ).asLong(), new Attributes( label, attributes ) )
        }
    }
}
