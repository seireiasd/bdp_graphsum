package graph.spark.parser;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SparkParser
{
    public static <T> JavaRDD<T> parseJson( JavaRDD<String> input, Function<JsonNode, T> schematizer )
    {
        return input.map( string -> schematizer.call( ( ( new ObjectMapper() ).readTree( string ) ) ) );
    }
}
