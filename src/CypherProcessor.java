import javafx.util.Pair;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.*;

public class CypherProcessor extends CypherBaseListener {

    String[] properties = {"name", "description", "source"};

    int implicit_nodeVar_cnt = 0;
    int implicit_connVar_cnt = 0;
    List<String> varList = new ArrayList<>();
    List<String> nodeVars = new ArrayList<>();
    Map<String, Set<String>> nodeTypeMap = new HashMap<>();
    Map<String, Set> relTypeMap = new HashMap<>();
    Map<String, Set> varConstraints = new HashMap<>();
    Map<String, String> directionMap = new HashMap<>();
    Map<String, Pair<Integer, Integer>> edgeLenMap = new HashMap<>();
    Map<String, Pair<String, String>> connMap = new HashMap<>();
    Map<String, List<Pair<String, String>>> nodeEdgesMap = new HashMap<>();


    List<String> connVars = new ArrayList<>();
    List<String> returnItems = new ArrayList<>();


    String whereClause;

    public static CypherProcessor process(String sentence) {
        // Get our lexer
        CypherLexer lexer = new CypherLexer(new ANTLRInputStream(sentence));

        // Get a list of matched tokens
        CommonTokenStream tokens = new CommonTokenStream(lexer);

        // Pass the tokens to the parser
        CypherParser parser = new CypherParser(tokens);

        // Specify our entry point
        CypherParser.OC_CypherContext ctx = parser.oC_Cypher();

        // Walk it and attach our listener
        ParseTreeWalker walker = new ParseTreeWalker();
        CypherProcessor listener = new CypherProcessor();
        walker.walk(listener, ctx);

        return listener;

    }

    @Override
    public void enterOC_Cypher(CypherParser.OC_CypherContext ctx) {
        //System.out.println("Cypher Statement:"+ctx.getText());
    }

    public void enterOC_ReturnItems(CypherParser.OC_ReturnItemsContext ctx) {
        //returnItems = ctx.getText();
        //System.out.println("Return items: "+ctx.getText());
    }

    public void enterOC_ReturnItem(CypherParser.OC_ReturnItemContext ctx) {
        returnItems.add(ctx.getText());
    }

    public String processRelationshipPattern(CypherParser.OC_RelationshipPatternContext ctx) {
        String text = ctx.getText();
        String var = (ctx.oC_RelationshipDetail().oC_Variable() != null) ? ctx.oC_RelationshipDetail().oC_Variable().getText() : "c" + String.valueOf(implicit_connVar_cnt++);
        connVars.add(var);

        String dir = String.valueOf(text.charAt(0)) + String.valueOf(text.charAt(text.length() - 1));
        String dirVal = "";
        if ("->".equals(dir)) {
            dirVal = "r";
        } else if ("<-".equals(dir)) {
            dirVal = "l";
        } else if ("--".equals(dir) || "<>".equals(dir)) {
            dirVal = "b";
        }
        directionMap.put(var, dirVal);

        if (ctx.oC_RelationshipDetail() != null && ctx.oC_RelationshipDetail().oC_RelationshipTypes() != null && ctx.oC_RelationshipDetail().oC_RelationshipTypes().oC_RelTypeName() != null) {
            Set<String> relTypes = new HashSet<>();
            for (CypherParser.OC_RelTypeNameContext relTypeNameContext : ctx.oC_RelationshipDetail().oC_RelationshipTypes().oC_RelTypeName()) {
                //relTypeConditions.add("relationship = '" + relTypeNameContext.getText() + "'");
                relTypes.add(relTypeNameContext.getText());
            }
            if (!relTypes.isEmpty())
                relTypeMap.put(var, relTypes);
        }

        int minChain = 1, maxChain = 1;
        if (ctx.oC_RelationshipDetail().oC_RangeLiteral() != null) {
            if (ctx.oC_RelationshipDetail().oC_RangeLiteral().oC_IntegerLiteral(0) != null) {
                minChain = Integer.parseInt(ctx.oC_RelationshipDetail().oC_RangeLiteral().oC_IntegerLiteral(0).getText());
            }
            if (ctx.oC_RelationshipDetail().oC_RangeLiteral().oC_IntegerLiteral(1) != null) {
                maxChain = Integer.parseInt(ctx.oC_RelationshipDetail().oC_RangeLiteral().oC_IntegerLiteral(1).getText());
            }
        }
        edgeLenMap.put(var, new Pair(minChain, maxChain));

        return var;
    }

    public void enterOC_PatternElement(CypherParser.OC_PatternElementContext ctx) {
        String a_node_var = "", b_node_var = "", conn_var = "";
        if (ctx.oC_NodePattern() != null) {
            a_node_var = processNodePattern(ctx.oC_NodePattern());
        }
        for (CypherParser.OC_PatternElementChainContext pattenElement : ctx.oC_PatternElementChain()) {
            b_node_var = processNodePattern(pattenElement.oC_NodePattern());
            conn_var = processRelationshipPattern(pattenElement.oC_RelationshipPattern());
            connMap.put(conn_var, new Pair(a_node_var, b_node_var));
            String dir = directionMap.get(conn_var);

            if (!nodeEdgesMap.containsKey(a_node_var)) {
                nodeEdgesMap.put(a_node_var, new ArrayList<>());
            }
            if (!nodeEdgesMap.containsKey(b_node_var)) {
                nodeEdgesMap.put(b_node_var, new ArrayList<>());
            }

            if (dir.equals("r")) {
                nodeEdgesMap.get(a_node_var).add(new Pair(conn_var, "o"));
                nodeEdgesMap.get(b_node_var).add(new Pair(conn_var, "i"));
            } else if (dir.equals("l")) {
                nodeEdgesMap.get(a_node_var).add(new Pair(conn_var, "i"));
                nodeEdgesMap.get(b_node_var).add(new Pair(conn_var, "o"));
            } else {
                nodeEdgesMap.get(a_node_var).add(new Pair(conn_var, "b"));
                nodeEdgesMap.get(b_node_var).add(new Pair(conn_var, "b"));
            }

            a_node_var = b_node_var;
        }

    }

    public String processNodePattern(CypherParser.OC_NodePatternContext ctx) {
        String var = (ctx.oC_Variable() != null) ? ctx.oC_Variable().getText() : "n" + String.valueOf(implicit_nodeVar_cnt++);
        if (!nodeVars.contains(var)) {
            nodeVars.add(var);
        }

        if (ctx.oC_NodeLabels() != null && ctx.oC_NodeLabels().oC_NodeLabel() != null && ctx.oC_NodeLabels().oC_NodeLabel().size() > 0) {
            Set<String> nodeTypes = new HashSet<>();
            if (nodeTypeMap.containsKey(var))
                nodeTypes = nodeTypeMap.get(var);
            for (CypherParser.OC_NodeLabelContext label : ctx.oC_NodeLabels().oC_NodeLabel()) {
                nodeTypes.add(label.oC_LabelName().getText());
            }
            nodeTypeMap.put(var, nodeTypes);
        }
        return var;
    }

    public void enterOC_Variable(CypherParser.OC_VariableContext ctx) {
        varList.add(ctx.getText());
    }

    public void enterOC_Where(CypherParser.OC_WhereContext ctx) {

        whereClause = ctx.oC_Expression().getText();
        String[] list = whereClause.split("(?i) and | or |\\(|\\)");

        String propertyRegex = "";
        for (String property : properties) {
            propertyRegex += "." + property + "|";
        }
        if (!propertyRegex.isEmpty()) {
            propertyRegex = propertyRegex.substring(0, propertyRegex.length() - 1);
        }


        for (String str : list) {
            str = str.trim();
            if (str.isEmpty())
                continue;

            String var = str.split(propertyRegex)[0];
            Set<String> constraints = new HashSet<>();
            if (varConstraints.containsKey(var))
                constraints = varConstraints.get(var);

            constraints.add(str);
            varConstraints.put(var, constraints);

        }
    }

}
