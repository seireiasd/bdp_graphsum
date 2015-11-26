package graph.spark.example;

import java.util.Iterator;
import java.util.Map.Entry;

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

            attributes.pairs.put( "label", node.get( "meta" ).get( "label" ).asText() );

            Iterator<Entry<String, JsonNode>> props = node.get( "data" ).fields();

            while ( props.hasNext() )
            {
                Entry<String, JsonNode> entry = props.next();

                if ( entry.getValue().isDouble() )
                {
                    attributes.pairs.put( entry.getKey(), new Double( entry.getValue().asDouble() ) );
                }
                else if ( entry.getValue().isInt() )
                {
                    attributes.pairs.put( entry.getKey(), new Integer( entry.getValue().asInt() ) );
                }
                else if ( entry.getValue().isTextual() )
                {
                    attributes.pairs.put( entry.getKey(), entry.getValue().asText() );
                }
                else
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

            attributes.pairs.put( "label", node.get( "meta" ).get( "label" ).asText() );

            Iterator<Entry<String, JsonNode>> props = node.get( "data" ).fields();

            while ( props.hasNext() )
            {
                Entry<String, JsonNode> entry = props.next();

                if ( entry.getValue().isDouble() )
                {
                    attributes.pairs.put( entry.getKey(), new Double( entry.getValue().asDouble() ) );
                }
                else if ( entry.getValue().isInt() )
                {
                    attributes.pairs.put( entry.getKey(), new Integer( entry.getValue().asInt() ) );
                }
                else if ( entry.getValue().isTextual() )
                {
                    attributes.pairs.put( entry.getKey(), entry.getValue().asText() );
                }
                else
                {
                    throw new RuntimeException( "Oh dears..." );
                }
            }

            return new Edge<GenericAttributes>( node.get( "source" ).asLong(), node.get( "target" ).asLong(), attributes );
        }
    }
}
