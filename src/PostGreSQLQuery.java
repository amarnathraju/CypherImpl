import javafx.util.Pair;
import model.ConnectionInfo;
import model.Edge;
import model.Node;

import java.sql.*;
import java.util.*;

public class PostGreSQLQuery {

    private final String url = "jdbc:postgresql://localhost/testdb";
    private final String user = "postgres";
    private final String password = "password";
    private Connection conn;

    public PostGreSQLQuery() {
        this.conn = connect();
    }

    /**
     * Connect to the PostgreSQL database
     *
     * @return a Connection object
     */
    private Connection connect() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            //System.out.println("Connected to the PostgreSQL server successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return conn;
    }

    public Set<ConnectionInfo> getConns(String sql) {
        Set<ConnectionInfo> list = new HashSet<>();
        ResultSet rs = executeSQL(sql);
        try {
            while (rs.next()) {
                ConnectionInfo info = new ConnectionInfo();

                info.setEdgeList((String[]) rs.getArray(1).getArray());
                info.setaNodeId(rs.getString(2));
                info.setbNodeId(rs.getString(3));
                list.add(info);
            }
        } catch (SQLException e) {
        }
        return list;
    }

    public Set<String> getIds(String sql) {
        Set<String> ids = new HashSet<>();
        ResultSet rs = executeSQL(sql);
        try {
            while (rs.next()) {
                ids.add(rs.getString(1));
            }
        } catch (SQLException e) {
        }
        return ids;
    }

    public List<List<Pair<String, String>>> getResultsFor(String sql) {
        List<List<Pair<String, String>>> resultList = new ArrayList<>();
        ResultSet rs = executeSQL(sql);
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                List<Pair<String, String>> row = new ArrayList<>();
                for (int i = 1; i <= columnsNumber; i++) {
                    String columnValue = rs.getString(i);
                    row.add(new Pair(rsmd.getColumnName(i), columnValue));
                    //System.out.print(rsmd.getColumnName(i) + ":\t" +columnValue + "\t" );
                }
                resultList.add(row);
                //System.out.println("");
            }
        } catch (SQLException e) {

        }
        return resultList;

    }

    public void printResultsFor(String sql) {
        ResultSet rs = executeSQL(sql);
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columnsNumber; i++) {
                    if (i > 1) System.out.print(",  ");
                    String columnValue = rs.getString(i);
                    System.out.print(rsmd.getColumnName(i) + ":\t" + columnValue + "\t");
                }
                System.out.println();
            }
        } catch (SQLException e) {

        }

    }

    private Map<String, String> processFuncQuery(String sql) {
        ResultSet rs = executeSQL(sql);
        Map<String, String> map = new HashMap<>();
        try {
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                String id = "", values = "";
                for (int i = 1; i <= columnsNumber; i++) {
                    String columnValue = rs.getString(i);
                    String columnName = rsmd.getColumnName(i);
                    if ("id".equals(columnName))
                        id = columnValue;
                    else
                        values = columnValue;
                }
                map.put(id, values);
            }
        } catch (SQLException e) {

        }
        return map;

    }

    public Map<String, String> getEdgeParam(List<String> edgeIds, String param) {
        String table = "edges", colname = "id";
        if ("relationship".equals(param)) {
            table = "connections";
            colname = "edge_id";
        }
        String sql = "Select " + colname + " as id, " + param + " from " + table + " where " + colname + " in ('" + String.join("' , '", edgeIds) + "');";
        return processFuncQuery(sql);
    }

    public Map<String, String> getDegreeForNodeIds(List<String> nodeIds, String inOrOut) {
        String colName = "";
        if ("IN".equalsIgnoreCase(inOrOut))
            colName = "b_node_id";
        else //if("OUT".equals(inOrOut))
            colName = "a_node_id";

        String sql = "Select " + colName + " as id, count(*) as cnt from connections where " + colName + " in  ('" + String.join("' , '", nodeIds) + "')  group by id ;";
        //select a_node_id as id, count(*) as cnt from connections where a_node_id in ('Node-1','Node-321') group by a_node_id order by cnt desc
        return processFuncQuery(sql);
    }

    public Map<String, String> getLabelsForNodeIds(List<String> nodeIds) {
        String sql = "Select id, string_agg(node_type, ',' order by node_type) as node_types from nodes where id in ('" + String.join("' , '", nodeIds) + "')  group by id; ";
        //System.out.println("Query for labels :\n"+sql);
        return processFuncQuery(sql);
    }

    public HashMap<String, Edge> getEdgesFromDB(Set edgeIds) {
        return getEdgesFromDB(edgeIds, false);
    }

    public HashMap<String, Edge> getEdgesFromDB(Set edgeIds, Boolean enrich) {
        HashMap<String, Edge> idToEdgeMap = new HashMap<>();
        if (edgeIds.isEmpty())
            return idToEdgeMap;
        String sql = "Select edge_id, relationship from connections where edge_id in ('" + String.join("' , '", edgeIds) + "')";
        ResultSet rs = executeSQL(sql);
        String id, relType;
        try {
            while (rs.next()) {
                id = rs.getString(1);
                relType = rs.getString(2);
                idToEdgeMap.put(id, new Edge(id, relType));
            }
        } catch (SQLException e) {
        }
        if (enrich) {
            sql = "Select id, source, description from edges where id in ('" + String.join("' , '", edgeIds) + "')";
            rs = executeSQL(sql);
            try {
                while (rs.next()) {
                    id = rs.getString(1);
                    idToEdgeMap.get(id).setSrc(rs.getString(2));
                    idToEdgeMap.get(id).setDesc(rs.getString(3));
                }
            } catch (SQLException e) {
            }

        }
        return idToEdgeMap;
    }

    public HashMap<String, Node> getNodesFromDB(Set nodeIds) {
        HashMap<String, Node> idToNodeMap = new HashMap<>();
        if (nodeIds.isEmpty())
            return idToNodeMap;
        String sql = "Select id, name, node_type from nodes where id in ('" + String.join("' , '", nodeIds) + "')";
        ResultSet rs = executeSQL(sql);
        String id, name, nodeType;
        Node node;
        try {
            while (rs.next()) {
                id = rs.getString(1);
                nodeType = rs.getString(3);
                if (!idToNodeMap.containsKey(id)) {
                    name = rs.getString(2);
                    node = new Node();
                    node.setId(id);
                    node.setName(name);
                    node.addLabel(nodeType);
                    idToNodeMap.put(id, node);
                } else {
                    idToNodeMap.get(id).addLabel(nodeType);
                }
            }
        } catch (SQLException e) {
        }
        return idToNodeMap;
    }


    protected ResultSet executeSQL(String sql) {
        System.out.println("DEBUG: " + sql);
        ResultSet rs = null;
        try {
            PreparedStatement st = conn.prepareStatement(sql);
            rs = st.executeQuery();

        } catch (SQLException e) {

        }
        return rs;
    }


}
