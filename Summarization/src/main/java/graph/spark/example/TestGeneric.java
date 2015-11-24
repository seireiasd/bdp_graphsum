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

import graph.spark.example.model.generic.GenericEdge;
import graph.spark.example.model.generic.GenericVertex;
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
            SparkConf               conf     = new SparkConf().setAppName( "Spark Test" ).setMaster( "local[1]" );
            JavaSparkContext        context  = new JavaSparkContext( conf );
            JavaRDD<GenericVertex>  vertices = SparkParser.parseJson( context.textFile( "data/nodes.json" ), new VertexMapper() );
            JavaRDD<GenericEdge>    edges    = SparkParser.parseJson( context.textFile( "data/edges.json" ), new EdgeMapper() );

            vertices.cache();
            edges.cache();

            System.out.println( "vertices: " + vertices.count() );
            System.out.println( "edges: " + edges.count() );

            System.out.println( "first Customer.name: " +
                                vertices.filter( vertex -> vertex.attributes.get( "label" ).equals( "Customer" ) )
                                        .collect()
                                        .get( 0 )
                                        .attributes.get( "name" ) );
            System.out.println( "first PurchOrderLine.quantity: " +
                                edges.filter( edge -> edge.attributes.get( "label" ).equals( "PurchOrderLine" ) )
                                     .collect()
                                     .get( 0 )
                                     .attributes.get( "quantity" ) );

            context.close();
        }
        catch ( Exception e )
        {
            System.out.println( e.toString() );
        }
    }

    private static class VertexMapper implements Function<JsonNode, GenericVertex>
    {
        private static final long serialVersionUID = 1L;

        @Override
        public GenericVertex call( JsonNode node )
        {
            GenericVertex vertex = new GenericVertex( node.get( "id" ).asInt() );

            vertex.attributes.put( "label", node.get( "meta" ).get( "label" ).asText() );

            Iterator<Entry<String, JsonNode>> props = node.get( "data" ).fields();

            while ( props.hasNext() )
            {
                Entry<String, JsonNode> entry = props.next();

                if ( entry.getValue().isDouble() )
                {
                    vertex.attributes.put( entry.getKey(), new Double( entry.getValue().asDouble() ) );
                }
                else if ( entry.getValue().isInt() )
                {
                    vertex.attributes.put( entry.getKey(), new Integer( entry.getValue().asInt() ) );
                }
                else if ( entry.getValue().isTextual() )
                {
                    vertex.attributes.put( entry.getKey(), entry.getValue().asText() );
                }
                else
                {
                    throw new RuntimeException( "Oh dears..." );
                }
            }

            return vertex;
        }
    }

    private static class EdgeMapper implements Function<JsonNode, GenericEdge>
    {
        private static final long serialVersionUID = 1L;

        @Override
        public GenericEdge call( JsonNode node )
        {
            GenericEdge edge = new GenericEdge( node.get( "source" ).asInt(), node.get( "target" ).asInt() );

            edge.attributes.put( "label", node.get( "meta" ).get( "label" ).asText() );

            Iterator<Entry<String, JsonNode>> props = node.get( "data" ).fields();

            while ( props.hasNext() )
            {
                Entry<String, JsonNode> entry = props.next();

                if ( entry.getValue().isDouble() )
                {
                    edge.attributes.put( entry.getKey(), new Double( entry.getValue().asDouble() ) );
                }
                else if ( entry.getValue().isInt() )
                {
                    edge.attributes.put( entry.getKey(), new Integer( entry.getValue().asInt() ) );
                }
                else if ( entry.getValue().isTextual() )
                {
                    edge.attributes.put( entry.getKey(), entry.getValue().asText() );
                }
                else
                {
                    throw new RuntimeException( "Oh dears..." );
                }
            }

            return edge;
        }
    }
}
