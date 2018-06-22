import javafx.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SQLCreator {

    private static String createOneSidedConnStr(String connTable, int minChain, int maxChain, Set<String> a_node_candidates, Set<String> b_node_candidates) {
        String connItem;
        if (minChain == maxChain) {
            connItem = "( " + createConnectionsJoin(connTable, minChain, a_node_candidates, b_node_candidates) + " )";
        } else {
            String prefix = "( " + createConnectionsJoin(connTable, minChain, a_node_candidates, b_node_candidates) + " )";
            for (int i = minChain + 1; i <= maxChain; i++) {
                prefix = prefix + " UNION ( " + createConnectionsJoin(connTable, i, a_node_candidates, b_node_candidates) + " )";
            }
            connItem = "( " + prefix + " )";
        }
        return connItem;
    }


    private static String createConnectionsJoin(String connTable, int n, Set<String> a_node_candidates, Set<String> b_node_candidates) {
        List<String> edgeIdsColList = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            edgeIdsColList.add("conn" + String.valueOf(i) + ".edge_id");
        }
        String prefix = "Select ARRAY[" + String.join(" , ", edgeIdsColList) + "] as edge_ids, conn0.a_node_id, conn" + String.valueOf(n - 1) + ".b_node_id, "
                + String.valueOf(n) + " as path_len from " + connTable + " as conn0 ";
        if (1 < n) {
            for (int i = 1; i < n; i++) {
                String joinStr = " inner join " + connTable + " as conn" + String.valueOf(i) + " on conn" + String.valueOf(i - 1) + ".b_node_id = conn" + String.valueOf(i) + ".a_node_id ";
                prefix = prefix + joinStr;
            }
        }
        if (!a_node_candidates.isEmpty() || !b_node_candidates.isEmpty()) {
            prefix = prefix + ((1 < n) ? " and " : "Where ");
            if (!a_node_candidates.isEmpty() && !b_node_candidates.isEmpty()) {
                prefix += " conn0.a_node_id in ( '" + String.join("' , '", a_node_candidates) + "' ) and conn" + String.valueOf(n - 1) +
                        ".b_node_id in ( '" + String.join("' , '", b_node_candidates) + "' )";
            } else if (!a_node_candidates.isEmpty()) {
                prefix += " conn0.a_node_id in ( '" + String.join("' , '", a_node_candidates) + "' )";
            } else if (!b_node_candidates.isEmpty()) {
                prefix += " conn" + String.valueOf(n - 1) + ".b_node_id in ( '" + String.join("' , '", b_node_candidates) + "' )";
            }
        }
        //todo : add conditions for empty lists.
        //System.out.println(prefix);
        return prefix;
    }


    private static String getPattern(String item) {
        String pattern = item.trim();
        pattern = pattern.substring(1, pattern.length() - 1);
        return pattern;
    }

    public static String preProcessConstraint(String constraint) {

        if (constraint.toLowerCase().contains("contains")) {
            String[] list = constraint.split("contains");
            return list[0] + " like '%" + getPattern(list[1]) + "%'";
        }
        if (constraint.toLowerCase().contains("starts with")) {
            String[] list = constraint.split("starts with");
            return list[0] + " like '" + getPattern(list[1]) + "%'";
        }
        if (constraint.toLowerCase().contains("ends with")) {
            String[] list = constraint.split("ends with");
            return list[0] + " like '%" + getPattern(list[1]) + "'";
        }

        return constraint;

    }

    public static String getSQLforNodeVar(String var, Set<String> labels, Set<String> whereConstraints, Set<String> nodeCandidates) {
        String sql = "Select distinct(id) as id from nodes as " + var;
        List<String> constraints = new ArrayList<>();
        for (String label : labels) {
            constraints.add("node_type = '" + label + "'");
        }
        constraints.addAll(whereConstraints);

        constraints = constraints.stream().map((constraint) -> preProcessConstraint(constraint)).collect(Collectors.toList());
        if (!constraints.isEmpty() || !nodeCandidates.isEmpty()) {
            sql += " where ";
            if (!constraints.isEmpty() && !nodeCandidates.isEmpty())
                sql += " ( " + String.join(" and ", constraints) + " ) and id in ( '" + String.join("','", nodeCandidates) + "' ) ";
            else if (!constraints.isEmpty())
                sql += " ( " + String.join(" and ", constraints) + " )";
            else
                sql += "id in ( '" + String.join("','", nodeCandidates) + "' ) ";
        }
        return sql;
    }

    public static String getSQLforConnVar(String var, Set<String> relTypes,
                                          Pair<Integer, Integer> lengthPair, String dir,
                                          Set<String> aNodeCandidates, Set<String> bNodeCandidates) {

        String connTable = " Select edge_id, a_node_id, b_node_id from connections as " + var;
        if (!relTypes.isEmpty()) {
            connTable = connTable + " where relationship in ( '" + String.join("' , '", relTypes) + "' ) ";
        }
        connTable = "( " + connTable + " )";

        int minChain = lengthPair.getKey(), maxChain = lengthPair.getValue();

        String sql;
        String leftQ = createOneSidedConnStr(connTable, minChain, maxChain, bNodeCandidates, aNodeCandidates);
        String rightQ = createOneSidedConnStr(connTable, minChain, maxChain, aNodeCandidates, bNodeCandidates);

        if ("l".equals(dir))
            sql = leftQ;
        else if ("r".equals(dir)) {
            sql = rightQ;
        } else {
            sql = "( " + leftQ + " union " + rightQ + " ) ";
        }
        return sql;

    }
}
