import javafx.util.Pair;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


// Direct translation of Cypher queries to SQL with out taking advantage of the graph structure of the data.
@Deprecated
public class CustomListenerOrg extends CypherBaseListener  {

    int implicit_nodeVar_cnt = 0;
    int implicit_connVar_cnt = 0;
    List<String> varList = new ArrayList<>();
    List<String> nodeVars = new ArrayList<>();
    List<String> connVars = new ArrayList<>();
    List<String> connItems = new ArrayList<>();
    List<String> directions = new ArrayList<>();
    List<String> whereCs = new ArrayList<>();
    List<String> returnItems = new ArrayList<>();
    List<String> customReturnItems = new ArrayList<>();
    PostGreSQLQuery app = new PostGreSQLQuery();


    @Override
    public void enterOC_Cypher(CypherParser.OC_CypherContext ctx) {
        System.out.println("Cypher Statement:"+ctx.getText());
    }

    public void enterOC_ReturnItems(CypherParser.OC_ReturnItemsContext ctx){
        //returnItems = ctx.getText();
        System.out.println("Return items: "+ctx.getText());
    }

    public void enterOC_ReturnItem(CypherParser.OC_ReturnItemContext ctx){
        returnItems.add(ctx.getText());
    }

    public void enterOC_RelationshipPattern(CypherParser.OC_RelationshipPatternContext ctx) {
        String text = ctx.getText();
        directions.add(String.valueOf(text.charAt(0)) + String.valueOf(text.charAt(text.length() -1)));
        List<String> relTypeConditions = new ArrayList<>();
        String var = (ctx.oC_RelationshipDetail().oC_Variable()  != null)? ctx.oC_RelationshipDetail().oC_Variable().getText() : "c"+String.valueOf(implicit_connVar_cnt++);
        connVars.add(var);
        if(ctx.oC_RelationshipDetail() != null && ctx.oC_RelationshipDetail().oC_RelationshipTypes() != null && ctx.oC_RelationshipDetail().oC_RelationshipTypes().oC_RelTypeName() != null) {
            for (CypherParser.OC_RelTypeNameContext relTypeNameContext : ctx.oC_RelationshipDetail().oC_RelationshipTypes().oC_RelTypeName()) {
                relTypeConditions.add("relationship = '" + relTypeNameContext.getText() + "'");
            }
        } else {
            relTypeConditions.add( "relationship like '%'");
        }

        String connTable = "( Select edge_id, a_node_id, b_node_id from connections where ( "+ String.join(" or ",relTypeConditions) +" ) )";

        int minChain = 1, maxChain =1;
        if(ctx.oC_RelationshipDetail().oC_RangeLiteral() != null){
            if(ctx.oC_RelationshipDetail().oC_RangeLiteral().oC_IntegerLiteral(0) != null){
                minChain = Integer.parseInt(ctx.oC_RelationshipDetail().oC_RangeLiteral().oC_IntegerLiteral(0).getText());
            }
            if(ctx.oC_RelationshipDetail().oC_RangeLiteral().oC_IntegerLiteral(1) != null){
                maxChain = Integer.parseInt(ctx.oC_RelationshipDetail().oC_RangeLiteral().oC_IntegerLiteral(1).getText());
            }
        }
        //Todo:Handle case where minChain > maxChain
        String connItem;
        if( minChain == maxChain){
            connItem = "( "+createConnectionsJoin(connTable, minChain)+" ) as "+ var;
        } else {
            String prefix = "( "+createConnectionsJoin(connTable, minChain)+" )";
            for(int i = minChain+1; i <= maxChain; i++ ){
                prefix = prefix +" UNION ( "+createConnectionsJoin(connTable, i)+" )";
            }
             connItem="( "+prefix+" ) as "+ var;
        }
        System.out.println("ConnItem :"+connItem);
        connItems.add(connItem);
    }

    private String createConnectionsJoin(String connTable, int n){
        List<String> edgeIdsColList = new ArrayList<>();
        for(int i =0 ; i <n ; i++){
            edgeIdsColList.add("conn"+String.valueOf(i)+".edge_id");
        }
        String prefix = "Select ARRAY["+ String.join(" , ",edgeIdsColList)+"] as edge_ids, conn0.a_node_id, conn"+String.valueOf(n-1)+".b_node_id, "+String.valueOf(n)+" as path_len from "+connTable+" as conn0 ";
        if(1 < n){
            for(int i = 1; i < n ; i++){
                String joinStr = " inner join "+connTable+" as conn"+String.valueOf(i)+" on conn"+String.valueOf(i-1)+".b_node_id = conn"+String.valueOf(i)+".a_node_id ";
                prefix = prefix + joinStr;
            }
        }
        System.out.println(prefix);
        return prefix;
    }

    public void enterOC_NodePattern(CypherParser.OC_NodePatternContext ctx) {
        String var = (ctx.oC_Variable()  != null)? ctx.oC_Variable().getText() : "n"+String.valueOf(implicit_nodeVar_cnt++);
        if( !nodeVars.contains(var)){
            nodeVars.add(var);
        }

        if(ctx.oC_NodeLabels() != null && ctx.oC_NodeLabels().oC_NodeLabel() != null && ctx.oC_NodeLabels().oC_NodeLabel().size() > 0 ){
            for(CypherParser.OC_NodeLabelContext label : ctx.oC_NodeLabels().oC_NodeLabel()){
                whereCs.add(var+".node_type = '" + label.oC_LabelName().getText()+ "'");
            }
        }
    }

    public void enterOC_Variable(CypherParser.OC_VariableContext ctx) {
        varList.add(ctx.getText());
    }

    public void enterOC_Where(CypherParser.OC_WhereContext ctx){
        whereCs.add(" ( " +ctx.oC_Expression().getText() + " ) ");
    }

    public static void main(String[] args) {
        String cypher = "MATCH (a:PERSON)-[]-(b:PERSON)-[]-(c:CLUB)-[]-(d)-[]-(e)-[]-(f)-[]-(g) WHERE a.name = 'DONALD J. TRUMP' and c.name= 'MAR-A-LAGO CLUB, INC' RETURN    a.name,b.name,c.name,d.name,e.name,f.name,g.name";

        //String cypher = "MATCH (a:PERSON)<-[e*1..3]->(c:CLUB) WHERE a.name = 'DONALD J. TRUMP' and c.name= 'MAR-A-LAGO CLUB, INC' RETURN  length(e), minLength(e)";
        //String cypher = "MATCH (a:PERSON)-[e*1..4]-(b:ORGANIZATION)-[f*1..4]-(c:PERSON) WHERE a.name = 'DONALD J. TRUMP' RETURN   a.name, b.name, c.name, length(e,f), minLength(e,f)";
        //String cypher = "MATCH (a:PERSON)-[e1]->(m:CLUB)<-[e2]-(b:PERSON) where a.name like 'DONALD J. TRUMP' RETURN a.name, m.name, b.name";
                //e1,e2,inDegree(a), outDegree(a), m.name, labels(m), b.name, inDegree(b), outDegree(b) ";
        CustomListenerOrg listenerOrg = new CustomListenerOrg();
        List<List<Pair<String,String>>> results = listenerOrg.process(cypher);

        for(List<Pair<String,String>> row: results){
            for(Pair<String,String> p : row){
                System.out.print(p.toString()+"\t");
            }
            System.out.println();
        }

    }

    private List<List<Pair<String,String>>> process(String sentence) {
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
        CustomListenerOrg listener = new CustomListenerOrg();
        walker.walk(listener, ctx);


        if(listener.directions.size() + 1 !=  listener.nodeVars.size()){
            System.out.println("SOMETHING is wrong with expected size of Connections and nodes");
        } else{
            List<String> directions = listener.directions;
            List<String> connections = listener.connVars;

            for (int i = 0 ; i < directions.size(); i++) {
                String dir = directions.get(i);
                String conn = connections.get(i);
                String rightEdge = "("+conn+".a_node_id = "+listener.nodeVars.get(i)+".id and "+ conn+".b_node_id ="+listener.nodeVars.get(i+1)+".id"+")";
                String leftEdge = "("+conn+".b_node_id = "+listener.nodeVars.get(i)+".id and "+ conn+".a_node_id ="+listener.nodeVars.get(i+1)+".id"+")";

                if("->".equals(dir)){
                    listener.whereCs.add(rightEdge);
                } else if("<-".equals(dir)){
                    listener.whereCs.add(leftEdge);
                } else if("--".equals(dir) || "<>".equals(dir)){
                    listener.whereCs.add("(" +leftEdge + "or" +rightEdge +")");
                }


            }
        }

        for (String c : listener.whereCs){
            System.out.println(c);
        }
        List<Boolean> isRetModified = new ArrayList<>();
        String labelPattern = "^labels\\([\\$_a-zA-Z]+[\\$_\\w]*\\)$";
        String inDegreePattern = "^inDegree\\([\\$_a-zA-Z]+[\\$_\\w]*\\)$";
        String outDegreePattern = "^outDegree\\([\\$_a-zA-Z]+[\\$_\\w]*\\)$";

        String descPattern = "^[\\$_a-zA-Z]+[\\$_\\w]*.(desc|description)$";
        String srcPattern = "^[\\$_a-zA-Z]+[\\$_\\w]*.(src|source)$";
        String relPattern = "^[\\$_a-zA-Z]+[\\$_\\w]*.(rel|relation|relationship)$";
        String minLenPattern = "^minLength\\(([\\$_a-zA-Z]+[\\$_\\w]*)(,([\\$_a-zA-Z]+[\\$_\\w]*))*\\)$";
        String lenPattern = "^length\\(([\\$_a-zA-Z]+[\\$_\\w]*)(,([\\$_a-zA-Z]+[\\$_\\w]*))*\\)$";

        for(String item : listener.returnItems){
            if(item.matches(labelPattern)){
                isRetModified.add(Boolean.TRUE);
                customReturnItems.add(item.substring(7,item.length() -1) +".id");
            } else if (item.matches(inDegreePattern)){
                isRetModified.add(Boolean.TRUE);
                customReturnItems.add(item.substring(9,item.length() -1) +".id");
            } else if (item.matches(outDegreePattern)) {
                isRetModified.add(Boolean.TRUE);
                customReturnItems.add(item.substring(10,item.length() -1) +".id");
            } else if (item.matches(descPattern) || item.matches(srcPattern) || item.matches(relPattern) ) {
                isRetModified.add(Boolean.TRUE);
                customReturnItems.add(item.split(Pattern.quote("."))[0] + ".edge_ids");
            } else if (item.matches(lenPattern)) {
                isRetModified.add(Boolean.TRUE);
                String[] edgeVarList = item.substring(7,item.length() -1).split(Pattern.quote(","));
                List<String> arrLenCall = new ArrayList<>();
                for(String edgeVar: edgeVarList){
                    arrLenCall.add("array_length("+edgeVar+".edge_ids , 1)");
                }
                customReturnItems.add(String.join(" + ",arrLenCall ));
            }else if (item.matches(minLenPattern)) {
                isRetModified.add(Boolean.TRUE);
                String[] edgeVarList = item.substring(10,item.length() -1).split(Pattern.quote(","));
                List<String> arrLenCall = new ArrayList<>();
                for(String edgeVar: edgeVarList){
                    arrLenCall.add("array_length("+edgeVar+".edge_ids , 1)");
                }
                customReturnItems.add("( "+edgeVarList[0]+".a_node_id, "+edgeVarList[edgeVarList.length -1]+".b_node_id,  "+String.join(" + ",arrLenCall )+" ) ");
            }

            else {
                isRetModified.add(Boolean.FALSE);
                customReturnItems.add(item);
            }

        }

        Boolean modifiedQuery = Boolean.FALSE;

        for(Boolean mod: isRetModified){
            modifiedQuery = Boolean.logicalOr(modifiedQuery, mod);
        }

        String sql = createSQL(listener.nodeVars, listener.connItems, listener.whereCs, customReturnItems);
        List<List<Pair<String,String>>> results = app.getResultsFor(sql);

        if(modifiedQuery){
            for(int i =0; i < customReturnItems.size(); i ++){
                if(isRetModified.get(i)){
                    String orgReturnItem = listener.returnItems.get(i);
                    if(orgReturnItem.contains("labels(") || orgReturnItem.contains("Degree(")){
                        List<String> nodeIds = new ArrayList<>();
                        for(List<Pair<String,String>> row : results){
                            nodeIds.add(row.get(i).getValue());
                        }
                        Map<String, String> map;
                        if(orgReturnItem.matches(labelPattern))
                            map = app.getLabelsForNodeIds(nodeIds);
                        else if(orgReturnItem.matches(inDegreePattern))
                            map = app.getDegreeForNodeIds(nodeIds, "IN");
                        else
                            map = app.getDegreeForNodeIds(nodeIds, "OUT");
                        for(int row_idx = 0 ; row_idx < results.size(); row_idx++) {
                            List<Pair<String,String>> row = results.get(row_idx);
                            row.set(i,new Pair(orgReturnItem,map.get(row.get(i).getValue())));
                            //nodeIds.add(row.get(i).getValue());
                        }
                    } else if(orgReturnItem.matches(descPattern) || orgReturnItem.matches(srcPattern) || orgReturnItem.matches(relPattern)) {
                        List<String> edgeIds = new ArrayList<>();
                        Pattern p = Pattern.compile("\"([^\"]*)\"");
                        for(List<Pair<String,String>> row : results){
                            Matcher m = p.matcher(row.get(i).getValue());
                            while(m.find()){
                                edgeIds.add(m.group(1));
                            }
                        }
                        Map<String, String> map ;
                        if(orgReturnItem.matches(descPattern))
                            map = app.getEdgeParam(edgeIds, "description");
                        else if(orgReturnItem.matches(srcPattern))
                            map = app.getEdgeParam(edgeIds, "source");
                        else
                            map = app.getEdgeParam(edgeIds, "relationship");

                        for(int row_idx = 0 ; row_idx < results.size(); row_idx++) {
                            List<Pair<String,String>> row = results.get(row_idx);
                            StringBuffer sb = new StringBuffer();
                            Matcher m = p.matcher(row.get(i).getValue());
                            while(m.find()){
                                String val = escapeMetaCharacters(map.get(m.group(1)));
                                if(val != null)
                                    m.appendReplacement(sb, val);
                            }
                            m.appendTail(sb);
                            row.set(i,new Pair(orgReturnItem,sb.toString()));
                        }
                    }else if(orgReturnItem.matches(lenPattern)){
                        for(int row_idx = 0 ; row_idx < results.size(); row_idx++) {
                            List<Pair<String,String>> row = results.get(row_idx);
                            row.set(i,new Pair(orgReturnItem,row.get(i).getValue()));
                        }
                    }
                    else if (orgReturnItem.matches(minLenPattern)){
                        List<String> keyList = new ArrayList<>();
                        Map<String, Integer> minLen = new HashMap<>();
                        for(List<Pair<String,String>> row : results){
                            String item =row.get(i).getValue() ;
                            String[] tupleElements = item.substring(1,item.length()-1).split(Pattern.quote(","));
                            String key = tupleElements[0] + tupleElements[1];
                            Integer val = Integer.parseInt(tupleElements[2]);
                            keyList.add(key);
                            if(!minLen.containsKey(key) || minLen.get(key) > val)
                                minLen.put(key,val);
                        }
                        for(int row_idx = 0 ; row_idx < results.size(); row_idx++) {
                            List<Pair<String,String>> row = results.get(row_idx);
                            row.set(i,new Pair(orgReturnItem,minLen.get(keyList.get(row_idx))));
                        }
                    }
                }
            }
        }
        return results;
    }

    public String escapeMetaCharacters(String inputString){
        final String[] metaCharacters = {"\\","^","$","{","}","[","]","(",")",".","*","+","?","|","<",">","-","&"};
        String outputString="";
        for (int i = 0 ; i < metaCharacters.length ; i++){
            if(inputString.contains(metaCharacters[i])){
                outputString = inputString.replace(metaCharacters[i],"\\"+metaCharacters[i]);
                inputString = outputString;
            }
        }
        return outputString;
    }
    private static String createSQL(List<String> nodeVars, List<String> connItems, List<String> whereCs, List<String> returnItems){
        List<String> fromCs = new ArrayList<>();
        for(String var : nodeVars)
            fromCs.add("nodes as "+var);

        String sql = "SELECT " + String.join(" , ", returnItems) +" FROM " + String.join(" , ", fromCs) +" , "+ String.join(" , ",connItems) + " WHERE "+ String.join(" and ", whereCs) + ";";
        return sql;

    }


//    private static String createSQL(List<String> nodeVars, List<String> connVars, List<String> whereCs, List<String> returnItems){
//        List<String> fromCs = new ArrayList<>();
//        for(String var : nodeVars)
//            fromCs.add("nodes as "+var);
//        for (String var: connVars)
//            fromCs.add("connections as "+var);
//
//        String sql = "SELECT " + String.join(" , ", returnItems) +" FROM " + String.join(" , ", fromCs) + " WHERE "+ String.join(" and ", whereCs) + ";";
//        return sql;
//
//    }

}
