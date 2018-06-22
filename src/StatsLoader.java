import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class StatsLoader extends PostGreSQLQuery {
    int Cn, Ce;
    Map<String, Integer> Clabel = new HashMap<>();
    Map<String, Integer> CrelType = new HashMap<>();
    Map<String, Integer> Cnameword = new HashMap<>();
    Map<String, Map<String, Integer>> CInlabelRel = new HashMap<>();
    Map<String, Map<String, Integer>> COutlabelRel = new HashMap<>();
    Map<String, Map<String, Integer>> CInNodeRel = new HashMap<>();
    Map<String, Map<String, Integer>> COutNodeRel = new HashMap<>();

    StatsLoader() {
        populateNodeAndEdgeCounts();
        populateLabelCounts();
        populateRelTypeCounts();
        populateLabelRelationCounts();
        populateNodeRelationCounts();
        populateNameWordCounts();
    }

    private void populateNameWordCounts() {
        String sql = "select * from nameword_count";
        this.Cnameword = getCountMap(sql);
    }

    private void populateNodeRelationCounts() {
        String inQuery = "select * from node_rel_in_count";
        String outQuery = "select * from node_rel_out_count";
        String node, relationship;
        int count;

        ResultSet rs = executeSQL(inQuery);
        try {
            while (rs.next()) {
                node = rs.getString(1);
                relationship = rs.getString(2);
                count = rs.getInt(3);
                if (!this.CInNodeRel.containsKey(node))
                    this.CInNodeRel.put(node, new HashMap<String, Integer>());
                this.CInNodeRel.get(node).put(relationship, count);
            }
        } catch (SQLException e) {
        }

        rs = executeSQL(outQuery);
        try {
            while (rs.next()) {
                node = rs.getString(1);
                relationship = rs.getString(2);
                count = rs.getInt(3);
                if (!this.COutNodeRel.containsKey(node))
                    this.COutNodeRel.put(node, new HashMap<String, Integer>());
                this.COutNodeRel.get(node).put(relationship, count);
            }
        } catch (SQLException e) {
        }


    }

    private void populateLabelRelationCounts() {
        String inQuery = "select * from label_rel_in_count";
        String outQuery = "select * from label_rel_out_count";
        String label, relationship;
        int count;

        ResultSet rs = executeSQL(inQuery);
        try {
            while (rs.next()) {
                label = rs.getString(1);
                relationship = rs.getString(2);
                count = rs.getInt(3);
                if (!this.CInlabelRel.containsKey(label))
                    this.CInlabelRel.put(label, new HashMap<String, Integer>());
                this.CInlabelRel.get(label).put(relationship, count);
            }
        } catch (SQLException e) {
        }

        rs = executeSQL(outQuery);
        try {
            while (rs.next()) {
                label = rs.getString(1);
                relationship = rs.getString(2);
                count = rs.getInt(3);
                if (!this.COutlabelRel.containsKey(label))
                    this.COutlabelRel.put(label, new HashMap<String, Integer>());
                this.COutlabelRel.get(label).put(relationship, count);
            }
        } catch (SQLException e) {
        }


    }

    private void populateRelTypeCounts() {
        String sql = "select * from rel_count";
        this.CrelType = getCountMap(sql);
    }

    private void populateLabelCounts() {
        String sql = "select * from label_count";
        this.Clabel = getCountMap(sql);
    }

    private Map<String, Integer> getCountMap(String sql) {
        Map<String, Integer> countMap = new HashMap<>();
        ResultSet rs = executeSQL(sql);
        try {
            while (rs.next()) {
                countMap.put(rs.getString(1), rs.getInt(2));
            }
        } catch (SQLException e) {
        }
        return countMap;
    }

    private void populateNodeAndEdgeCounts() {
        String CnQuery = "Select count(*) from nodes";
        String CeQuery = "Select count(*) from edges";

        ResultSet rs = executeSQL(CnQuery);
        try {
            if (rs.next()) {
                this.Cn = rs.getInt(1);
            }
        } catch (SQLException e) {
        }

        rs = executeSQL(CeQuery);
        try {
            if (rs.next()) {
                this.Ce = rs.getInt(1);
            }
        } catch (SQLException e) {
        }

    }

}
