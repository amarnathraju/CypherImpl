package model;

public class ConnectionInfo {
    private String[] edgeList ;
    private String aNodeId, bNodeId;

    public String[] getEdgeList() {
        return edgeList;
    }

    public void setEdgeList(String[] edgeList) {
        this.edgeList = edgeList;
    }

    public String getaNodeId() {
        return aNodeId;
    }

    public void setaNodeId(String aNodeId) {
        this.aNodeId = aNodeId;
    }

    public String getbNodeId() {
        return bNodeId;
    }

    public void setbNodeId(String bNodeId) {
        this.bNodeId = bNodeId;
    }
}
