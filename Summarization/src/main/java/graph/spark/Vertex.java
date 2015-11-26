package graph.spark;

import java.io.Serializable;

public class Vertex<VD extends Serializable> implements Serializable
{
    private static final long serialVersionUID = 1L;

    private final long mVertexID;
    private final VD   mVertexData;

    public Vertex( long vertexID, VD vertexData )
    {
        mVertexID   = vertexID;
        mVertexData = vertexData;
    }

    public long getVertexID()
    {
        return mVertexID;
    }

    public VD getData()
    {
        return mVertexData;
    }
}
