package graph.spark;

import java.io.Serializable;

public class Edge<ED extends Serializable> implements Serializable
{
    private static final long serialVersionUID = 1L;

    private final long mSourceID;
    private final long mTargetID;
    private final ED   mEdgeData;

    public Edge( long sourceID, long targetID, ED edgeData )
    {
        mSourceID = sourceID;
        mTargetID = targetID;
        mEdgeData = edgeData;
    }

    public long getSourceID()
    {
        return mSourceID;
    }

    public long getTargetID()
    {
        return mTargetID;
    }

    public ED getData()
    {
        return mEdgeData;
    }
}
