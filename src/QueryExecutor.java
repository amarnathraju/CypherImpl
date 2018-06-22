import javafx.util.Pair;
import model.ConnectionInfo;
import model.Edge;
import model.Node;
import org.antlr.v4.runtime.misc.Triple;

import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryExecutor {

    StatsLoader statsLoader;
    CypherProcessor cp;
    PostGreSQLQuery dbApp;

    Map<String, Set<String>> nodeCandidatesMap;
    Map<String, Set<ConnectionInfo>> connCandidatesMap;
    Map<Triple<String, String, String>, List<ConnectionInfo>> nodeConnMap;
    Map<String, Object> graph;
    Set<Map<String, Object>> resultGraphs;

    Set<String> nodesToConsider;
    Set<String> connsToConsider;
    Set<String> queriedNodes;
    Set<String> queriedConns;

    HashMap<String, Node> idToNodeMap;
    HashMap<String, Edge> idToEdgeMap;

    Map<Pair<Map, List>, Integer> lenCache;

    Comparator<Map.Entry<String, Integer>> cardinalityComparator = new Comparator<Map.Entry<String, Integer>>() {
        @Override
        public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
            return o1.getValue().compareTo(o2.getValue());
        }
    };

    Map<String, Integer> nodeCardinality;
    Map<String, Integer> connCardinality;


    QueryExecutor() {
        this.statsLoader = new StatsLoader();
        this.dbApp = new PostGreSQLQuery();
    }

    public static <T> boolean containsUnique(Stream<T> stream) {
        Set<T> set = new HashSet<>();
        return stream.allMatch(t -> set.add(t));
    }

    public static void main(String[] args) {
        QueryExecutor executor = new QueryExecutor();
        Scanner sc = new Scanner(System.in);
        String cypher;

        while (true) {
            System.out.println("\n\n\n");
            System.out.println("Enter your query (or 'quit'):\n\n\n");
            cypher = sc.nextLine();
            if (cypher.equals("exit") || cypher.equals("quit")) {
                break;
            }
            try {
                executor.execute(cypher);
            } catch (Exception e){
                e.printStackTrace();
            }
        }

    }

    private Set<String> getNodesForVar(String var) {
        String sql = SQLCreator.getSQLforNodeVar(var, cp.nodeTypeMap.getOrDefault(var, new HashSet<>()),
                cp.varConstraints.getOrDefault(var, new HashSet()), nodeCandidatesMap.getOrDefault(var, new HashSet<>()));
        return dbApp.getIds(sql);
    }

    private Set<ConnectionInfo> getConnForVar(String var) {
        String sql = SQLCreator.getSQLforConnVar(var, cp.relTypeMap.getOrDefault(var, new HashSet()),
                cp.edgeLenMap.get(var), cp.directionMap.get(var),
                nodeCandidatesMap.getOrDefault(cp.connMap.get(var).getKey(), new HashSet<>()),
                nodeCandidatesMap.getOrDefault(cp.connMap.get(var).getValue(), new HashSet<>()));
        return dbApp.getConns(sql);

    }

    private void queryDB() {
        this.nodesToConsider.addAll(cp.nodeVars);
        this.connsToConsider = new HashSet<>();
        this.queriedNodes = new HashSet<>();
        this.queriedConns = new HashSet<>();
        String minNodeVar;
        while (!nodesToConsider.isEmpty()) {
            minNodeVar = extractNodeWithMinCard();
            processNodeVar(minNodeVar);
        }
    }

    private void processNodeVar(String nodeVar) {
        nodeCandidatesMap.put(nodeVar, getNodesForVar(nodeVar));
        queriedNodes.add(nodeVar);
        nodesToConsider.remove(nodeVar);
        Set<String> nodeConns = cp.nodeEdgesMap.getOrDefault(nodeVar, new ArrayList<>()).stream().map(pair -> pair.getKey()).filter(conn -> !queriedConns.contains(conn)).collect(Collectors.toSet());
        connsToConsider.addAll(nodeConns);
        updateConnCard(nodeConns);
        String minConnVar = extractConnWithMinCard();
        if (!minConnVar.isEmpty()) {
            String otherNodeVar = getOtherNodeVar(nodeVar, minConnVar);
            processConnVar(minConnVar, otherNodeVar);
        }
    }

    private void updateConnCard(Set<String> conns) {
        for (String var : conns) {
            if (!connCardinality.containsKey(var)) {
                connCardinality.put(var, calcConnCard(var));
            }
        }
        connCardinality.keySet().retainAll(connsToConsider);
    }

    private String extractConnWithMinCard() {
        connCardinality.keySet().retainAll(connsToConsider);
        return connCardinality.isEmpty() ? "" : connCardinality.entrySet().stream().min(cardinalityComparator).get().getKey();
    }

    private String getOtherNodeVar(String nodeVar, String connVar) {
        String node1 = cp.connMap.get(connVar).getKey();
        String node2 = cp.connMap.get(connVar).getValue();
        if (node1.equals(nodeVar))
            return node2;
        else
            return node1;
    }

    private void processConnVar(String connVar, String otherNodeVar) {
        connCandidatesMap.put(connVar, getConnForVar(connVar));
        queriedConns.add(connVar);
        connsToConsider.remove(connVar);
        updateCandidatesForNodeWRTConnVar(otherNodeVar, connVar);
        if (!queriedNodes.contains(otherNodeVar)) {
            processNodeVar(otherNodeVar);
        }
    }

    private void updateCandidatesForNodeWRTConnVar(String nodeVar, String connVar) {
        String node1 = cp.connMap.get(connVar).getKey();
        String dir = cp.directionMap.get(connVar);
        Stream<String> stream;
        if ((node1.equals(nodeVar) && dir.equals("r")) || (!node1.equals(nodeVar) && dir.equals("l")))
            stream = connCandidatesMap.get(connVar).stream().map(info -> info.getaNodeId());
        else
            stream = connCandidatesMap.get(connVar).stream().map(info -> info.getbNodeId());
        if (dir.equals("b")) {
            stream = Stream.concat(connCandidatesMap.get(connVar).stream().map(info -> info.getaNodeId()), connCandidatesMap.get(connVar).stream().map(info -> info.getbNodeId()));
        }
        Set<String> newNodeCandidates = nodeCandidatesMap.containsKey(nodeVar) ? stream.filter(nodeCandidatesMap.get(nodeVar)::contains).collect(Collectors.toSet()) : stream.collect(Collectors.toSet());
        nodeCandidatesMap.put(nodeVar, newNodeCandidates);
    }

    private Integer calcConnCard(String connVar) {
        String node1 = cp.connMap.get(connVar).getKey();
        String node2 = cp.connMap.get(connVar).getValue();
        String dir = cp.directionMap.get(connVar);
        Integer minLen = cp.edgeLenMap.get(connVar).getKey();
        Integer maxLen = cp.edgeLenMap.get(connVar).getValue();
        Set<String> relTypes = cp.relTypeMap.getOrDefault(connVar, new HashSet());
        if ("r".equals(dir)) {
            return connectionCard(node1, node2, relTypes, maxLen)
                    - connectionCard(node1, node2, relTypes, minLen - 1);
        } else if ("l".equals(dir)) {
            return connectionCard(node2, node1, relTypes, maxLen)
                    - connectionCard(node2, node1, relTypes, minLen - 1);
        } else {
            return (connectionCard(node1, node2, relTypes, maxLen) + connectionCard(node2, node1, relTypes, maxLen))
                    - (connectionCard(node1, node2, relTypes, minLen - 1) + connectionCard(node2, node1, relTypes, minLen - 1));
        }
    }

    private Integer connectionCard(String aNodeVar, String bNodeVar, Set<String> relTypes, Integer length) {
        if (length == 0) {
            return 0;
        } else if (length == 1) {
            return Math.min(outEdgeNodeCard(aNodeVar, relTypes), inEdgeNodeCard(bNodeVar, relTypes));
        } else if (length == 2) {
            return outEdgeNodeCard(aNodeVar, relTypes) * inEdgeNodeCard(bNodeVar, relTypes);
        } else {
            int M = (relTypes.isEmpty()) ? statsLoader.Ce : (relTypes.stream().map(relType -> statsLoader.CrelType.get(relType)).reduce(Integer::sum).get());

            return outEdgeNodeCard(aNodeVar, relTypes) * inEdgeNodeCard(bNodeVar, relTypes) * (int) Math.round(Math.pow(M, length - 2));
        }
    }

    private Integer outEdgeNodeCard(String nodeVar, Set<String> relTypes) {
        return getOneSidedEdgeCard(nodeVar, relTypes, "out");
    }

    private Integer inEdgeNodeCard(String nodeVar, Set<String> relTypes) {
        return getOneSidedEdgeCard(nodeVar, relTypes, "in");
    }

    private Integer getOneSidedEdgeCard(String nodeVar, Set<String> relTypes, String inOrOut) {
        Map<String, Map<String, Integer>> nodeRelCounts, labelRelCounts;
        if ("in".equals(inOrOut)) {
            nodeRelCounts = statsLoader.CInNodeRel;
            labelRelCounts = statsLoader.CInlabelRel;
        } else {
            nodeRelCounts = statsLoader.COutNodeRel;
            labelRelCounts = statsLoader.COutlabelRel;
        }
        Integer oneSidedCard = 0;
        if (nodeCandidatesMap.containsKey(nodeVar)) {
            if (relTypes.isEmpty()) {
                oneSidedCard = nodeCandidatesMap.get(nodeVar).stream().
                        map(nodeId -> nodeRelCounts.containsKey(nodeId) ? nodeRelCounts.get(nodeId).values().stream().reduce(Integer::sum).get() : 0).reduce(Integer::sum).get();
            } else {
                for (String relType : relTypes) {
                    oneSidedCard += nodeCandidatesMap.get(nodeVar).stream().map(nodeId ->
                            (nodeRelCounts.containsKey(nodeId) && nodeRelCounts.get(nodeId).containsKey(relType)) ? nodeRelCounts.get(nodeId).get(relType) : 0).reduce(Integer::sum).get();
                }
            }

        } else {
            if (relTypes.isEmpty()) {
                if (!cp.nodeTypeMap.getOrDefault(nodeVar, new HashSet<>()).isEmpty())
                    oneSidedCard = cp.nodeTypeMap.get(nodeVar).stream().map(label ->
                            labelRelCounts.get(label).values().stream().reduce(Integer::sum).get()).reduce(Integer::min).get();
                else {
                    oneSidedCard = statsLoader.Ce;
                }
            } else {
                for (String relType : relTypes) {
                    if (!cp.nodeTypeMap.getOrDefault(nodeVar, new HashSet<>()).isEmpty())
                        oneSidedCard += cp.nodeTypeMap.get(nodeVar).stream().map(label ->
                                (labelRelCounts.containsKey(label) && labelRelCounts.get(label).containsKey(relType)) ? labelRelCounts.get(label).get(relType) : 0).reduce(Integer::min).get();
                    else {
                        oneSidedCard += statsLoader.CrelType.get(relType);
                    }
                }
            }
        }
        return oneSidedCard;
    }

    private String extractNodeWithMinCard() {

        Set<String> whereConstraints, labels;
        for (String var : nodesToConsider) {
            if (!nodeCardinality.containsKey(var)) {
                Float P = 1f;
                whereConstraints = cp.varConstraints.getOrDefault(var, new HashSet());
                for (String constraint : whereConstraints) {
                    if (constraint.contains(".name") && constraint.contains("=")) {
                        P *= (1f / statsLoader.Cn);
                    }
                    if (constraint.contains(".name")) {
                        Boolean checkNameWords = Boolean.FALSE;
                        String matStr = "";
                        if (constraint.toLowerCase().contains("contains")) {
                            int qIndex = constraint.toLowerCase().indexOf("contains");
                            matStr = constraint.substring(qIndex + "contains".length()).trim();
                            checkNameWords = Boolean.TRUE;
                        } else if (constraint.toLowerCase().contains("starts with")) {
                            int qIndex = constraint.toLowerCase().indexOf("starts with");
                            matStr = constraint.substring(qIndex + "starts with".length()).trim();
                            checkNameWords = Boolean.TRUE;
                        } else if (constraint.toLowerCase().contains("ends with")) {
                            int qIndex = constraint.toLowerCase().indexOf("ends with");
                            matStr = constraint.substring(qIndex + "ends with".length()).trim();
                            checkNameWords = Boolean.TRUE;
                        }
                        if (checkNameWords == Boolean.TRUE) {
                            matStr = matStr.substring(1, matStr.length() - 1);
                            String[] terms = matStr.split("\\'|-|\\s");
                            for (String term : terms) {
                                if (statsLoader.Cnameword.containsKey(term))
                                    P *= ((float) statsLoader.Cnameword.get(term) / statsLoader.Cn);
                            }
                        }
                    }
                }
                labels = cp.nodeTypeMap.getOrDefault(var, new HashSet<>());
                for (String label : labels) {
                    P *= ((float) (statsLoader.Clabel.getOrDefault(label, 0)) / statsLoader.Cn);
                }

                nodeCardinality.put(var, Math.round(P * statsLoader.Cn));
            }
        }
        //todo: correct this
        nodeCardinality.keySet().retainAll(nodesToConsider);
        return nodeCardinality.entrySet().stream().min(cardinalityComparator).get().getKey();
    }

    private void generateNodeConnMap() {
        for (Map.Entry<String, Set<ConnectionInfo>> entry : connCandidatesMap.entrySet()) {
            String edgeVar = entry.getKey();
            Pair<String, String> pair = cp.connMap.get(edgeVar);
            String aNodeVar = pair.getKey();
            String bNodeVar = pair.getValue();

            Set<ConnectionInfo> connectionInfos = entry.getValue();
            for (ConnectionInfo info : connectionInfos) {
                String aNode = info.getaNodeId();
                String bNode = info.getbNodeId();
                Triple aTriple = new Triple(aNode, edgeVar, "o");
                Triple bTriple = new Triple(bNode, edgeVar, "i");
                if (!nodeConnMap.containsKey(aTriple))
                    nodeConnMap.put(aTriple, new ArrayList<ConnectionInfo>());
                if (!nodeConnMap.containsKey(bTriple))
                    nodeConnMap.put(bTriple, new ArrayList<ConnectionInfo>());

                nodeConnMap.get(aTriple).add(info);
                nodeConnMap.get(bTriple).add(info);
            }
        }
    }

    private void filterNodeCandidates() {
        //Todo: optimize order
        for (String edgeVar : cp.connVars) {
            String dir = cp.directionMap.get(edgeVar);
            String node1 = cp.connMap.get(edgeVar).getKey();
            String node2 = cp.connMap.get(edgeVar).getValue();

            if ("b".equals(dir)) {
                nodeCandidatesMap.put(node1, nodeCandidatesMap.get(node1).stream().filter(nodeVal -> nodeConnMap.containsKey(new Triple<>(nodeVal, edgeVar, "o")) || nodeConnMap.containsKey(new Triple<>(nodeVal, edgeVar, "i"))).collect(Collectors.toSet()));
                nodeCandidatesMap.put(node2, nodeCandidatesMap.get(node2).stream().filter(nodeVal -> nodeConnMap.containsKey(new Triple<>(nodeVal, edgeVar, "o")) || nodeConnMap.containsKey(new Triple<>(nodeVal, edgeVar, "i"))).collect(Collectors.toSet()));
            } else if ("l".equals(dir)) {
                nodeCandidatesMap.put(node1, nodeCandidatesMap.get(node1).stream().
                        filter(nodeVal -> nodeConnMap.containsKey(new Triple<>(nodeVal, edgeVar, "i"))).collect(Collectors.toSet()));
                nodeCandidatesMap.put(node2, nodeCandidatesMap.get(node2).stream().
                        filter(nodeVal -> nodeConnMap.containsKey(new Triple<>(nodeVal, edgeVar, "o"))).collect(Collectors.toSet()));
                /*nodeCandidatesMap.put(node1,
                        nodeCandidatesMap.get(node1).stream().filter(nodeVal ->
                                !(connCandidatesMap.get(edgeVar).stream().
                                        filter(info -> info.getaNodeId().equals(nodeVal) || info.getbNodeId().equals(nodeVal))
                                        .collect(Collectors.toSet()).isEmpty())
                        ).collect(Collectors.toSet()));

                nodeCandidatesMap.put(node2, nodeCandidatesMap.get(node2).stream().filter(nodeVal ->
                        !(connCandidatesMap.get(edgeVar).stream().filter(info -> info.getaNodeId().equals(nodeVal) || info.getbNodeId().equals(nodeVal)).collect(Collectors.toSet()).isEmpty())
                ).collect(Collectors.toSet()));
                */
            } else if ("r".equals(dir)) {
                //nodeCandidatesMap.put(node1, nodeCandidatesMap.get(node1).stream().filter(nodeVal  -> nodeConnMap.containsKey(new Triple<>(nodeVal, edgeVar, "i"))).collect(Collectors.toSet()));
                //nodeCandidatesMap.put(node2, nodeCandidatesMap.get(node2).stream().filter(nodeVal  -> nodeConnMap.containsKey(new Triple<>(nodeVal, edgeVar, "o"))).collect(Collectors.toSet()));
                nodeCandidatesMap.put(node1, nodeCandidatesMap.get(node1).stream().filter(nodeVal ->
                        !(connCandidatesMap.get(edgeVar).stream().filter(info -> info.getaNodeId().equals(nodeVal)
                                || info.getbNodeId().equals(nodeVal)).collect(Collectors.toSet()).isEmpty())
                ).collect(Collectors.toSet()));

                nodeCandidatesMap.put(node2, nodeCandidatesMap.get(node2).stream().filter(nodeVal ->
                        !(connCandidatesMap.get(edgeVar).stream().filter(info -> info.getaNodeId().equals(nodeVal)
                                || info.getbNodeId().equals(nodeVal)).collect(Collectors.toSet()).isEmpty())
                ).collect(Collectors.toSet()));
            }
        }

    }

    private void filterConnCandidates() {

        for (String edgeVar : cp.connVars) {
            Set<ConnectionInfo> candidates = connCandidatesMap.get(edgeVar);
            String nodeVar1 = cp.connMap.get(edgeVar).getKey();
            String nodeVar2 = cp.connMap.get(edgeVar).getValue();
            Set<String> node1Candidates = nodeCandidatesMap.get(nodeVar1);
            Set<String> node2Candidates = nodeCandidatesMap.get(nodeVar2);

            String dir = cp.directionMap.get(edgeVar);
            if ("b".equals(dir))
                candidates = candidates.stream().filter(info -> (node1Candidates.contains(info.getaNodeId()) && node2Candidates.contains(info.getbNodeId())) || (node1Candidates.contains(info.getbNodeId()) && node2Candidates.contains(info.getaNodeId()))).collect(Collectors.toSet());
            else if ("r".equals(dir))
                candidates = candidates.stream().filter(info -> (node1Candidates.contains(info.getaNodeId()) && node2Candidates.contains(info.getbNodeId()))).collect(Collectors.toSet());
            else if ("l".equals(dir))
                candidates = candidates.stream().filter(info -> (node1Candidates.contains(info.getbNodeId()) && node2Candidates.contains(info.getaNodeId()))).collect(Collectors.toSet());
            connCandidatesMap.put(edgeVar, candidates);
        }
    }

    private void getResultGraphs() {

        if (connCandidatesMap.values().stream().anyMatch(list -> list.isEmpty()) || nodeCandidatesMap.values().stream().anyMatch(list -> list.isEmpty()))
            return;

        String minNodeVar = cp.nodeVars.get(0);
        int minSize = Integer.MAX_VALUE;
        for (String nodeVar : cp.nodeVars) {
            if (nodeCandidatesMap.get(nodeVar).size() < minSize) {
                minSize = nodeCandidatesMap.get(nodeVar).size();
                minNodeVar = nodeVar;

            }
        }

        graph = new HashMap<>();

        for (String val : nodeCandidatesMap.get(minNodeVar)) {
            populateNodeVal(minNodeVar, val, null, null);
        }

        while (true) {
            Optional<Map<String, Object>> unfilledGraphOpt = resultGraphs.stream().filter(graph -> {
                Set<String> connVarsUnfilled = new HashSet<>(cp.connVars);
                connVarsUnfilled.removeAll(graph.keySet());
                return !connVarsUnfilled.isEmpty();
            }).findAny();

            if (!unfilledGraphOpt.isPresent()) {
                break;
            } else {
                fullFillGraph(unfilledGraphOpt.get());
            }
        }

//
//        for(Map graph : resultGraphs){
//            List<String> connVarsUnfilled = new ArrayList<>(cp.connVars);
//            connVarsUnfilled.removeAll(graph.keySet());
//            for(String connVar : connVarsUnfilled){
//                String aNodeVar = cp.connMap.get(connVar).getKey();
//                String bNodeVar = cp.connMap.get(connVar).getValue();
//
//                Function<ConnectionInfo, Boolean> rEdgeCond = ( connectionInfo  -> connectionInfo.getaNodeId().equals(graph.get(aNodeVar)) && connectionInfo.getbNodeId().equals(graph.get(bNodeVar)));
//                Function<ConnectionInfo, Boolean> lEdgeCond = ( connectionInfo  -> connectionInfo.getaNodeId().equals(graph.get(bNodeVar)) && connectionInfo.getbNodeId().equals(graph.get(aNodeVar)));
//                List<ConnectionInfo> infoList;
//                if("r".equals(cp.directionMap.get(connVar)))
//                    infoList = connCandidatesMap.get(connVar).stream().filter(info ->  rEdgeCond.apply(info) ).collect( Collectors.toList() );
//                else if("l".equals(cp.directionMap.get(connVar)))
//                    infoList = connCandidatesMap.get(connVar).stream().filter(info ->  lEdgeCond.apply(info) ).collect( Collectors.toList() );
//                else
//                    infoList = connCandidatesMap.get(connVar).stream().filter(info ->  lEdgeCond.apply(info) || rEdgeCond.apply(info)).collect( Collectors.toList() );
//                //Todo: Fix this create a graph for each value of infoList.
//                if(!infoList.isEmpty()){
//                    graph.put(connVar, infoList.get(0));
//                }else
//                    break;
//
//            }
//        }
        resultGraphs = resultGraphs.stream().filter(graph -> graph.keySet().containsAll(cp.nodeVars) && graph.keySet().containsAll(cp.connVars)).collect(Collectors.toSet());

    }

    public void fullFillGraph(Map g) {
        List<String> connVarsUnfilled = new ArrayList<>(cp.connVars);
        connVarsUnfilled.removeAll(g.keySet());
        for (String connVar : connVarsUnfilled) {
            String aNodeVar = cp.connMap.get(connVar).getKey();
            String bNodeVar = cp.connMap.get(connVar).getValue();

            Function<ConnectionInfo, Boolean> rEdgeCond = (connectionInfo -> connectionInfo.getaNodeId().equals(g.get(aNodeVar)) && connectionInfo.getbNodeId().equals(g.get(bNodeVar)));
            Function<ConnectionInfo, Boolean> lEdgeCond = (connectionInfo -> connectionInfo.getaNodeId().equals(g.get(bNodeVar)) && connectionInfo.getbNodeId().equals(g.get(aNodeVar)));
            List<ConnectionInfo> infoList;
            if ("r".equals(cp.directionMap.get(connVar)))
                infoList = connCandidatesMap.get(connVar).stream().filter(info -> rEdgeCond.apply(info)).collect(Collectors.toList());
            else if ("l".equals(cp.directionMap.get(connVar)))
                infoList = connCandidatesMap.get(connVar).stream().filter(info -> lEdgeCond.apply(info)).collect(Collectors.toList());
            else
                infoList = connCandidatesMap.get(connVar).stream().filter(info -> lEdgeCond.apply(info) || rEdgeCond.apply(info)).collect(Collectors.toList());
            //Todo: Fix this create a graph for each value of infoList.
            if (infoList.isEmpty()) {
                resultGraphs.remove(g);
            } else {
                for (ConnectionInfo info : infoList) {
                    g.put(connVar, info);
                    resultGraphs.add(new HashMap<>(g));
                }
            }
        }
    }

    private void populateNodeVal(String var, String val, String eVar, ConnectionInfo eInfo) {

        graph.put(var, val);
        if (eVar != null)
            graph.put(eVar, eInfo);
        if (graph.keySet().containsAll(cp.nodeVars)) {
            if (containsUnique(graph.values().stream())) {
                resultGraphs.add(new HashMap<>(graph));

            }
            graph.remove(var);
            if (eVar != null)
                graph.remove(eVar);
            return;
        }
        Set<Pair<String, String>> edgePairs = cp.nodeEdgesMap.getOrDefault(var, new ArrayList<>()).stream()
                .filter(pair -> !graph.containsKey(pair.getKey())).collect(Collectors.toSet());

        if (edgePairs.isEmpty()) {
            List<String> nodeVars = cp.nodeVars.stream().filter(nv -> !graph.containsKey(nv)).collect(Collectors.toList());
            if (!nodeVars.isEmpty()) {
                String minNodeVar = nodeVars.get(0);
                int minSize = Integer.MAX_VALUE;

                for (String nodeVar : nodeVars) {
                    if (nodeCandidatesMap.get(nodeVar).size() < minSize) {
                        minSize = nodeCandidatesMap.get(nodeVar).size();
                        minNodeVar = nodeVar;

                    }
                }

                for (String nodeVal : nodeCandidatesMap.get(minNodeVar)) {
                    populateNodeVal(minNodeVar, nodeVal, null, null);
                }
            }
        } else {
            for (Pair<String, String> pair : edgePairs) {
                String edgeType = pair.getValue();
                String edgeVar = pair.getKey();
                String neighbor = getOtherNodeVar(var, edgeVar);

                if (edgeType.equals("o")) {
                    if (!graph.containsKey(neighbor)) {
                        for (ConnectionInfo info : connCandidatesMap.get(edgeVar)) {
                            if (info.getaNodeId().equals(val) && nodeCandidatesMap.get(neighbor).contains(info.getbNodeId())) {
                                populateNodeVal(neighbor, info.getbNodeId(), edgeVar, info);
                                graph.remove(neighbor);
                                graph.remove(edgeVar);
                            }
                        }
                    }
                } else if (edgeType.equals("i")) {
                    if (!graph.containsKey(neighbor)) {
                        for (ConnectionInfo info : connCandidatesMap.get(edgeVar)) {
                            if (info.getbNodeId().equals(val) && nodeCandidatesMap.get(neighbor).contains(info.getaNodeId())) {
                                populateNodeVal(neighbor, info.getaNodeId(), edgeVar, info);
                                graph.remove(neighbor);
                                graph.remove(edgeVar);
                            }
                        }
                    }
                } else {
                    if (!graph.containsKey(neighbor)) {
                        for (ConnectionInfo info : connCandidatesMap.get(edgeVar)) {
                            if (info.getbNodeId().equals(val) && nodeCandidatesMap.get(neighbor).contains(info.getaNodeId())) {
                                populateNodeVal(neighbor, info.getaNodeId(), edgeVar, info);
                                graph.remove(neighbor);
                                graph.remove(edgeVar);
                            } else if (info.getaNodeId().equals(val) && nodeCandidatesMap.get(neighbor).contains(info.getbNodeId())) {
                                populateNodeVal(neighbor, info.getbNodeId(), edgeVar, info);
                                graph.remove(neighbor);
                                graph.remove(edgeVar);
                            }
                        }
                    }
                }
            }
        }


    }

    public void execute(String cypher) {
        this.cp = CypherProcessor.process(cypher);

        this.nodeCandidatesMap = new HashMap<>();
        this.connCandidatesMap = new HashMap<>();
        this.nodeConnMap = new HashMap<Triple<String, String, String>, List<ConnectionInfo>>();
        this.resultGraphs = new HashSet<>();

        this.nodesToConsider = new HashSet<>();
        this.connsToConsider = new HashSet<>();
        this.queriedNodes = new HashSet<>();
        this.queriedConns = new HashSet<>();

        this.nodeCardinality = new HashMap<>();
        this.connCardinality = new HashMap<>();


        queryDB();
        generateNodeConnMap();

        filterNodeCandidates();
        filterConnCandidates();

        getResultGraphs();

        List<List<String>> results = renderResults();

        printResults(results);
        System.out.println("Total number of results: " + results.size());
    }

    private void printResults(List<List<String>> results) {

        for (List result : results) {
            System.out.println(String.join(" , ", result));
        }

    }

    public List<List<String>> renderResults() {
        Set<String> nodeIds = new HashSet<>();
        Set<String> edgeIds = new HashSet<>();
        Boolean descOrSrc = Boolean.FALSE;
        List<List<String>> results = new ArrayList<>();

        for (Map g : resultGraphs) {
            Set<String> gNodeIds = (Set<String>) g.keySet().stream().filter(var -> cp.nodeVars.contains(var)).map(var -> g.get(var).toString()).collect(Collectors.toSet());
            nodeIds.addAll(gNodeIds);
            Set<String> gEdgeIds = (Set<String>) g.keySet().stream().filter(var -> cp.connVars.contains(var)).
                    flatMap(var -> Arrays.stream(((ConnectionInfo) g.get(var)).getEdgeList())).collect(Collectors.toSet());
            edgeIds.addAll(gEdgeIds);
        }

        idToNodeMap = dbApp.getNodesFromDB(nodeIds);
        for (String item : cp.returnItems) {
            if (item.contains("src") || item.contains("source") || item.contains("desc") || item.contains("description") || cp.connVars.contains(item)) {
                descOrSrc = Boolean.TRUE;
                break;
            }
        }
        if (descOrSrc)
            idToEdgeMap = dbApp.getEdgesFromDB(edgeIds, true);
        else
            idToEdgeMap = dbApp.getEdgesFromDB(edgeIds);

        lenCache = new HashMap<>();

        for (Map g : resultGraphs) {
            List result = new ArrayList<>();
            for (String returnItem : cp.returnItems) {
                result.add(getResultItem(g, returnItem));
            }
            results.add(result);
        }
        return results;
    }

    private String getResultItem(Map g, String returnItem) {
        returnItem = returnItem.trim();

        String labelPattern = "^labels\\([\\$_a-zA-Z]+[\\$_\\w]*\\)$";
        String inDegreePattern = "^inDegree\\([\\$_a-zA-Z]+[\\$_\\w]*\\)$";
        String outDegreePattern = "^outDegree\\([\\$_a-zA-Z]+[\\$_\\w]*\\)$";
        String minLenPattern = "^minLength\\(([\\$_a-zA-Z]+[\\$_\\w]*)(,([\\$_a-zA-Z]+[\\$_\\w]*))*\\)$";
        String lenPattern = "^length\\(([\\$_a-zA-Z]+[\\$_\\w]*)(,([\\$_a-zA-Z]+[\\$_\\w]*))*\\)$";

        String var;

        if (returnItem.contains(".")) {
            String[] parts = returnItem.split(Pattern.quote("."));
            var = parts[0];
            String param = parts[1];
            if (cp.nodeVars.contains(var)) {
                if (param.equals("name"))
                    return idToNodeMap.get(g.get(var)).getName();
            } else if (cp.connVars.contains(var)) {
                ConnectionInfo info = (ConnectionInfo) g.get(var);
                Stream<Edge> stream = Arrays.stream(info.getEdgeList()).map(id -> idToEdgeMap.get(id));
                if (param.equals("connection") || param.equals("rel") || param.equals("relation") || param.equals("relationship") || param.equals("relationship_type"))
                    return " [" + String.join(" , ", stream.map(edge -> edge.getRelType()).collect(Collectors.toList())) + "] ";
                if (param.equals("src") || param.equals("source"))
                    return " ['" + String.join("' , '", stream.map(edge -> edge.getSrc()).collect(Collectors.toList())) + "'] ";
                if (param.equals("description") || param.equals("desc"))
                    return " ['" + String.join("' , '", stream.map(edge -> edge.getDesc()).collect(Collectors.toList())) + "'] ";
            }
        } else if(cp.nodeVars.contains(returnItem)) {
            Node n = idToNodeMap.get(g.get(returnItem));
            return "{ name="+n.getName()+", labels="+n.getLabels().toString()+" }";
        } else if(cp.connVars.contains(returnItem)) {
            ConnectionInfo info = (ConnectionInfo) g.get(returnItem);
            Stream<Edge> stream = Arrays.stream(info.getEdgeList()).map(id -> idToEdgeMap.get(id));
            return " ['" + String.join("' , '", stream.map(edge -> edge.getDesc()).collect(Collectors.toList())) + "'] ";
        } else {
            if (returnItem.matches(labelPattern)) {
                var = returnItem.substring(7, returnItem.length() - 1).trim();
                if (cp.nodeVars.contains(var)) {
                    return idToNodeMap.get(g.get(var)).getLabels().toString();
                } else {
                    throw new IllegalArgumentException("Labels exist for Node variables only");
                }
            } else if (returnItem.matches(inDegreePattern)) {
                var = returnItem.substring(9, returnItem.length() - 1).trim();
                if (cp.nodeVars.contains(var)) {
                    return String.valueOf((Integer) (statsLoader.CInNodeRel.containsKey(g.get(var)) ?
                            statsLoader.CInNodeRel.get(g.get(var)).values().stream().reduce(Integer::sum).get() : 0));
                } else {
                    throw new IllegalArgumentException("InDegree exists for Node variables only");
                }
            } else if (returnItem.matches(outDegreePattern)) {
                var = returnItem.substring(10, returnItem.length() - 1).trim();
                if (cp.nodeVars.contains(var)) {
                    return String.valueOf((Integer)(statsLoader.COutNodeRel.containsKey(g.get(var)) ?
                            statsLoader.COutNodeRel.get(g.get(var)).values().stream().reduce(Integer::sum).get() : 0));
                } else {
                    throw new IllegalArgumentException("OutDegree exists for Node variables only");
                }
            } else if (returnItem.matches(lenPattern)) {
                String[] vars = returnItem.substring(7, returnItem.length() - 1).trim().split(",");
                if (cp.connVars.containsAll(Arrays.asList(vars)) || cp.nodeVars.containsAll(Arrays.asList(vars))) {
                    if (cp.nodeVars.containsAll(Arrays.asList(vars))) {
                        vars = getConnVarsForNodeVars(vars);
                    }
                    Integer len = getPathLength(g, vars);
                    return String.valueOf(len);
                } else {
                    throw new IllegalArgumentException("Variables should either be all Node Variables or all Connection variables");
                }

            } else if (returnItem.matches(minLenPattern)) {
                String[] vars = returnItem.substring(10, returnItem.length() - 1).trim().split(",");
                if (cp.connVars.containsAll(Arrays.asList(vars)) || cp.nodeVars.containsAll(Arrays.asList(vars))) {
                    if (cp.nodeVars.containsAll(Arrays.asList(vars))) {
                        vars = getConnVarsForNodeVars(vars);
                    }

                    Integer minLen = Integer.MAX_VALUE;
                    Integer lenTemp;
                    for (Map gr : resultGraphs) {
                        if (areAllNodeValsSame(gr, g)) {
                            lenTemp = getPathLength(gr, vars);
                            if (lenTemp < minLen)
                                minLen = lenTemp;
                        }
                    }
                    return String.valueOf(minLen);
                } else {
                    throw new IllegalArgumentException("Variables should either be all Node Variables or all Connection variables");
                }
            }

        }
        return "";

    }

    private Boolean areAllNodeValsSame(Map<String, Object> g1, Map<String, Object> g2) {
        return cp.nodeVars.stream().map(var -> g1.get(var).equals(g2.get(var))).reduce(Boolean::logicalAnd).get();
    }

    private Integer getPathLength(Map<String, Object> g, String[] edgeVars) {
        Integer len;
        List<String> edgeVarList = Arrays.asList(edgeVars);
        if (lenCache.containsKey(new Pair<>(g, edgeVarList))) {
            len = lenCache.get(new Pair<>(g, edgeVarList));
        } else {
            len = Arrays.stream(edgeVars).map(v -> ((ConnectionInfo) g.get(v)).getEdgeList().length).reduce(Integer::sum).get();

            lenCache.put(new Pair<>(g, edgeVarList), len);
        }

        return len;
    }

    private String[] getConnVarsForNodeVars(String[] nodeVars) {
        List<String> edgeVarsList = new ArrayList<>();
        for (int i = 0; i < nodeVars.length - 1; i++) {
            for (String k : cp.connMap.keySet()) {
                String dir = cp.directionMap.get(k);
                if (dir.equals("b") && (cp.connMap.get(k).equals(new Pair<>(nodeVars[i], nodeVars[i + 1])) || cp.connMap.get(k).equals(new Pair<>(nodeVars[i + 1], nodeVars[i])))) {
                    edgeVarsList.add(k);
                    break;
                } else if (dir.equals("r") && cp.connMap.get(k).equals(new Pair<>(nodeVars[i], nodeVars[i + 1]))) {
                    edgeVarsList.add(k);
                    break;
                } else if (dir.equals("l") && cp.connMap.get(k).equals(new Pair<>(nodeVars[i + 1], nodeVars[i]))) {
                    edgeVarsList.add(k);
                    break;
                }
            }
        }
        String[] edgeVars = new String[edgeVarsList.size()];
        edgeVars = edgeVarsList.toArray(edgeVars);

        return edgeVars;
    }
}
