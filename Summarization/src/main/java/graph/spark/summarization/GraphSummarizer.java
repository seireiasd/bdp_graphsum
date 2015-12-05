package graph.spark.summarization;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;

import graph.spark.Vertex;

public class GraphSummarizer
{
    public static <VD extends Serializable, ED extends Serializable>
    void summarize( JavaRDD<Vertex<VD>> vertices, JavaRDD<Vertex<ED>> edges )
    {
        ;
    }
}
