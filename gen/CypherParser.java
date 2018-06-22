// Generated from /home/raju/Documents/291H-GraphData/CypherJava/CypherOrg/Cypher.g4 by ANTLR 4.7
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class CypherParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.7", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, T__16=17, 
		T__17=18, T__18=19, T__19=20, T__20=21, T__21=22, T__22=23, T__23=24, 
		T__24=25, T__25=26, T__26=27, T__27=28, T__28=29, T__29=30, T__30=31, 
		T__31=32, T__32=33, T__33=34, T__34=35, T__35=36, T__36=37, T__37=38, 
		T__38=39, T__39=40, T__40=41, T__41=42, T__42=43, T__43=44, T__44=45, 
		UNION=46, ALL=47, OPTIONAL=48, MATCH=49, UNWIND=50, AS=51, MERGE=52, ON=53, 
		CREATE=54, SET=55, DETACH=56, DELETE=57, REMOVE=58, CALL=59, YIELD=60, 
		WITH=61, DISTINCT=62, RETURN=63, ORDER=64, BY=65, L_SKIP=66, LIMIT=67, 
		ASCENDING=68, ASC=69, DESCENDING=70, DESC=71, WHERE=72, OR=73, XOR=74, 
		AND=75, NOT=76, IN=77, STARTS=78, ENDS=79, CONTAINS=80, IS=81, NULL=82, 
		COUNT=83, FILTER=84, EXTRACT=85, ANY=86, NONE=87, SINGLE=88, TRUE=89, 
		FALSE=90, EXISTS=91, CASE=92, ELSE=93, END=94, WHEN=95, THEN=96, StringLiteral=97, 
		EscapedChar=98, HexInteger=99, DecimalInteger=100, OctalInteger=101, HexLetter=102, 
		HexDigit=103, Digit=104, NonZeroDigit=105, NonZeroOctDigit=106, OctDigit=107, 
		ZeroDigit=108, ExponentDecimalReal=109, RegularDecimalReal=110, CONSTRAINT=111, 
		DO=112, FOR=113, REQUIRE=114, UNIQUE=115, MANDATORY=116, SCALAR=117, OF=118, 
		ADD=119, DROP=120, UnescapedSymbolicName=121, IdentifierStart=122, IdentifierPart=123, 
		EscapedSymbolicName=124, SP=125, WHITESPACE=126, Comment=127;
	public static final int
		RULE_oC_Cypher = 0, RULE_oC_Statement = 1, RULE_oC_Query = 2, RULE_oC_RegularQuery = 3, 
		RULE_oC_Union = 4, RULE_oC_SingleQuery = 5, RULE_oC_SinglePartQuery = 6, 
		RULE_oC_MultiPartQuery = 7, RULE_oC_UpdatingClause = 8, RULE_oC_ReadingClause = 9, 
		RULE_oC_Match = 10, RULE_oC_Unwind = 11, RULE_oC_Merge = 12, RULE_oC_MergeAction = 13, 
		RULE_oC_Create = 14, RULE_oC_Set = 15, RULE_oC_SetItem = 16, RULE_oC_Delete = 17, 
		RULE_oC_Remove = 18, RULE_oC_RemoveItem = 19, RULE_oC_InQueryCall = 20, 
		RULE_oC_StandaloneCall = 21, RULE_oC_YieldItems = 22, RULE_oC_YieldItem = 23, 
		RULE_oC_With = 24, RULE_oC_Return = 25, RULE_oC_ReturnBody = 26, RULE_oC_ReturnItems = 27, 
		RULE_oC_ReturnItem = 28, RULE_oC_Order = 29, RULE_oC_Skip = 30, RULE_oC_Limit = 31, 
		RULE_oC_SortItem = 32, RULE_oC_Where = 33, RULE_oC_Pattern = 34, RULE_oC_PatternPart = 35, 
		RULE_oC_AnonymousPatternPart = 36, RULE_oC_PatternElement = 37, RULE_oC_NodePattern = 38, 
		RULE_oC_PatternElementChain = 39, RULE_oC_RelationshipPattern = 40, RULE_oC_RelationshipDetail = 41, 
		RULE_oC_Properties = 42, RULE_oC_RelationshipTypes = 43, RULE_oC_NodeLabels = 44, 
		RULE_oC_NodeLabel = 45, RULE_oC_RangeLiteral = 46, RULE_oC_LabelName = 47, 
		RULE_oC_RelTypeName = 48, RULE_oC_Expression = 49, RULE_oC_OrExpression = 50, 
		RULE_oC_XorExpression = 51, RULE_oC_AndExpression = 52, RULE_oC_NotExpression = 53, 
		RULE_oC_ComparisonExpression = 54, RULE_oC_AddOrSubtractExpression = 55, 
		RULE_oC_MultiplyDivideModuloExpression = 56, RULE_oC_PowerOfExpression = 57, 
		RULE_oC_UnaryAddOrSubtractExpression = 58, RULE_oC_StringListNullOperatorExpression = 59, 
		RULE_oC_PropertyOrLabelsExpression = 60, RULE_oC_Atom = 61, RULE_oC_Literal = 62, 
		RULE_oC_BooleanLiteral = 63, RULE_oC_ListLiteral = 64, RULE_oC_PartialComparisonExpression = 65, 
		RULE_oC_ParenthesizedExpression = 66, RULE_oC_RelationshipsPattern = 67, 
		RULE_oC_FilterExpression = 68, RULE_oC_IdInColl = 69, RULE_oC_FunctionInvocation = 70, 
		RULE_oC_FunctionName = 71, RULE_oC_ExplicitProcedureInvocation = 72, RULE_oC_ImplicitProcedureInvocation = 73, 
		RULE_oC_ProcedureResultField = 74, RULE_oC_ProcedureName = 75, RULE_oC_Namespace = 76, 
		RULE_oC_ListComprehension = 77, RULE_oC_PatternComprehension = 78, RULE_oC_PropertyLookup = 79, 
		RULE_oC_CaseExpression = 80, RULE_oC_CaseAlternatives = 81, RULE_oC_Variable = 82, 
		RULE_oC_NumberLiteral = 83, RULE_oC_MapLiteral = 84, RULE_oC_Parameter = 85, 
		RULE_oC_PropertyExpression = 86, RULE_oC_PropertyKeyName = 87, RULE_oC_IntegerLiteral = 88, 
		RULE_oC_DoubleLiteral = 89, RULE_oC_SchemaName = 90, RULE_oC_ReservedWord = 91, 
		RULE_oC_SymbolicName = 92, RULE_oC_LeftArrowHead = 93, RULE_oC_RightArrowHead = 94, 
		RULE_oC_Dash = 95;
	public static final String[] ruleNames = {
		"oC_Cypher", "oC_Statement", "oC_Query", "oC_RegularQuery", "oC_Union", 
		"oC_SingleQuery", "oC_SinglePartQuery", "oC_MultiPartQuery", "oC_UpdatingClause", 
		"oC_ReadingClause", "oC_Match", "oC_Unwind", "oC_Merge", "oC_MergeAction", 
		"oC_Create", "oC_Set", "oC_SetItem", "oC_Delete", "oC_Remove", "oC_RemoveItem", 
		"oC_InQueryCall", "oC_StandaloneCall", "oC_YieldItems", "oC_YieldItem", 
		"oC_With", "oC_Return", "oC_ReturnBody", "oC_ReturnItems", "oC_ReturnItem", 
		"oC_Order", "oC_Skip", "oC_Limit", "oC_SortItem", "oC_Where", "oC_Pattern", 
		"oC_PatternPart", "oC_AnonymousPatternPart", "oC_PatternElement", "oC_NodePattern", 
		"oC_PatternElementChain", "oC_RelationshipPattern", "oC_RelationshipDetail", 
		"oC_Properties", "oC_RelationshipTypes", "oC_NodeLabels", "oC_NodeLabel", 
		"oC_RangeLiteral", "oC_LabelName", "oC_RelTypeName", "oC_Expression", 
		"oC_OrExpression", "oC_XorExpression", "oC_AndExpression", "oC_NotExpression", 
		"oC_ComparisonExpression", "oC_AddOrSubtractExpression", "oC_MultiplyDivideModuloExpression", 
		"oC_PowerOfExpression", "oC_UnaryAddOrSubtractExpression", "oC_StringListNullOperatorExpression", 
		"oC_PropertyOrLabelsExpression", "oC_Atom", "oC_Literal", "oC_BooleanLiteral", 
		"oC_ListLiteral", "oC_PartialComparisonExpression", "oC_ParenthesizedExpression", 
		"oC_RelationshipsPattern", "oC_FilterExpression", "oC_IdInColl", "oC_FunctionInvocation", 
		"oC_FunctionName", "oC_ExplicitProcedureInvocation", "oC_ImplicitProcedureInvocation", 
		"oC_ProcedureResultField", "oC_ProcedureName", "oC_Namespace", "oC_ListComprehension", 
		"oC_PatternComprehension", "oC_PropertyLookup", "oC_CaseExpression", "oC_CaseAlternatives", 
		"oC_Variable", "oC_NumberLiteral", "oC_MapLiteral", "oC_Parameter", "oC_PropertyExpression", 
		"oC_PropertyKeyName", "oC_IntegerLiteral", "oC_DoubleLiteral", "oC_SchemaName", 
		"oC_ReservedWord", "oC_SymbolicName", "oC_LeftArrowHead", "oC_RightArrowHead", 
		"oC_Dash"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "';'", "','", "'='", "'+='", "'-'", "'*'", "'('", "')'", "'['", 
		"']'", "':'", "'|'", "'..'", "'+'", "'/'", "'%'", "'^'", "'<>'", "'<'", 
		"'>'", "'<='", "'>='", "'.'", "'{'", "'}'", "'$'", "'\u27E8'", "'\u3008'", 
		"'\uFE64'", "'\uFF1C'", "'\u27E9'", "'\u3009'", "'\uFE65'", "'\uFF1E'", 
		"'\u00AD'", "'\u2010'", "'\u2011'", "'\u2012'", "'\u2013'", "'\u2014'", 
		"'\u2015'", "'\u2212'", "'\uFE58'", "'\uFE63'", "'\uFF0D'", null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		"'0'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, null, null, null, "UNION", "ALL", 
		"OPTIONAL", "MATCH", "UNWIND", "AS", "MERGE", "ON", "CREATE", "SET", "DETACH", 
		"DELETE", "REMOVE", "CALL", "YIELD", "WITH", "DISTINCT", "RETURN", "ORDER", 
		"BY", "L_SKIP", "LIMIT", "ASCENDING", "ASC", "DESCENDING", "DESC", "WHERE", 
		"OR", "XOR", "AND", "NOT", "IN", "STARTS", "ENDS", "CONTAINS", "IS", "NULL", 
		"COUNT", "FILTER", "EXTRACT", "ANY", "NONE", "SINGLE", "TRUE", "FALSE", 
		"EXISTS", "CASE", "ELSE", "END", "WHEN", "THEN", "StringLiteral", "EscapedChar", 
		"HexInteger", "DecimalInteger", "OctalInteger", "HexLetter", "HexDigit", 
		"Digit", "NonZeroDigit", "NonZeroOctDigit", "OctDigit", "ZeroDigit", "ExponentDecimalReal", 
		"RegularDecimalReal", "CONSTRAINT", "DO", "FOR", "REQUIRE", "UNIQUE", 
		"MANDATORY", "SCALAR", "OF", "ADD", "DROP", "UnescapedSymbolicName", "IdentifierStart", 
		"IdentifierPart", "EscapedSymbolicName", "SP", "WHITESPACE", "Comment"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "Cypher.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public CypherParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class OC_CypherContext extends ParserRuleContext {
		public OC_StatementContext oC_Statement() {
			return getRuleContext(OC_StatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(CypherParser.EOF, 0); }
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_CypherContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Cypher; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Cypher(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Cypher(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Cypher(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_CypherContext oC_Cypher() throws RecognitionException {
		OC_CypherContext _localctx = new OC_CypherContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_oC_Cypher);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(193);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(192);
				match(SP);
				}
			}

			setState(195);
			oC_Statement();
			setState(200);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
			case 1:
				{
				setState(197);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(196);
					match(SP);
					}
				}

				setState(199);
				match(T__0);
				}
				break;
			}
			setState(203);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(202);
				match(SP);
				}
			}

			setState(205);
			match(EOF);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_StatementContext extends ParserRuleContext {
		public OC_QueryContext oC_Query() {
			return getRuleContext(OC_QueryContext.class,0);
		}
		public OC_StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Statement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Statement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Statement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_StatementContext oC_Statement() throws RecognitionException {
		OC_StatementContext _localctx = new OC_StatementContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_oC_Statement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(207);
			oC_Query();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_QueryContext extends ParserRuleContext {
		public OC_RegularQueryContext oC_RegularQuery() {
			return getRuleContext(OC_RegularQueryContext.class,0);
		}
		public OC_StandaloneCallContext oC_StandaloneCall() {
			return getRuleContext(OC_StandaloneCallContext.class,0);
		}
		public OC_QueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Query; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Query(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Query(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Query(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_QueryContext oC_Query() throws RecognitionException {
		OC_QueryContext _localctx = new OC_QueryContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_oC_Query);
		try {
			setState(211);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(209);
				oC_RegularQuery();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(210);
				oC_StandaloneCall();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_RegularQueryContext extends ParserRuleContext {
		public OC_SingleQueryContext oC_SingleQuery() {
			return getRuleContext(OC_SingleQueryContext.class,0);
		}
		public List<OC_UnionContext> oC_Union() {
			return getRuleContexts(OC_UnionContext.class);
		}
		public OC_UnionContext oC_Union(int i) {
			return getRuleContext(OC_UnionContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_RegularQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_RegularQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_RegularQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_RegularQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_RegularQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_RegularQueryContext oC_RegularQuery() throws RecognitionException {
		OC_RegularQueryContext _localctx = new OC_RegularQueryContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_oC_RegularQuery);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(213);
			oC_SingleQuery();
			setState(220);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,6,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(215);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(214);
						match(SP);
						}
					}

					setState(217);
					oC_Union();
					}
					} 
				}
				setState(222);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,6,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_UnionContext extends ParserRuleContext {
		public TerminalNode UNION() { return getToken(CypherParser.UNION, 0); }
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public TerminalNode ALL() { return getToken(CypherParser.ALL, 0); }
		public OC_SingleQueryContext oC_SingleQuery() {
			return getRuleContext(OC_SingleQueryContext.class,0);
		}
		public OC_UnionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Union; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Union(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Union(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Union(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_UnionContext oC_Union() throws RecognitionException {
		OC_UnionContext _localctx = new OC_UnionContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_oC_Union);
		int _la;
		try {
			setState(235);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(223);
				match(UNION);
				setState(224);
				match(SP);
				setState(225);
				match(ALL);
				setState(227);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(226);
					match(SP);
					}
				}

				setState(229);
				oC_SingleQuery();
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(230);
				match(UNION);
				setState(232);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(231);
					match(SP);
					}
				}

				setState(234);
				oC_SingleQuery();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_SingleQueryContext extends ParserRuleContext {
		public OC_SinglePartQueryContext oC_SinglePartQuery() {
			return getRuleContext(OC_SinglePartQueryContext.class,0);
		}
		public OC_MultiPartQueryContext oC_MultiPartQuery() {
			return getRuleContext(OC_MultiPartQueryContext.class,0);
		}
		public OC_SingleQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_SingleQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_SingleQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_SingleQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_SingleQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_SingleQueryContext oC_SingleQuery() throws RecognitionException {
		OC_SingleQueryContext _localctx = new OC_SingleQueryContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_oC_SingleQuery);
		try {
			setState(239);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,10,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(237);
				oC_SinglePartQuery();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(238);
				oC_MultiPartQuery();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_SinglePartQueryContext extends ParserRuleContext {
		public OC_ReturnContext oC_Return() {
			return getRuleContext(OC_ReturnContext.class,0);
		}
		public List<OC_ReadingClauseContext> oC_ReadingClause() {
			return getRuleContexts(OC_ReadingClauseContext.class);
		}
		public OC_ReadingClauseContext oC_ReadingClause(int i) {
			return getRuleContext(OC_ReadingClauseContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public List<OC_UpdatingClauseContext> oC_UpdatingClause() {
			return getRuleContexts(OC_UpdatingClauseContext.class);
		}
		public OC_UpdatingClauseContext oC_UpdatingClause(int i) {
			return getRuleContext(OC_UpdatingClauseContext.class,i);
		}
		public OC_SinglePartQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_SinglePartQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_SinglePartQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_SinglePartQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_SinglePartQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_SinglePartQueryContext oC_SinglePartQuery() throws RecognitionException {
		OC_SinglePartQueryContext _localctx = new OC_SinglePartQueryContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_oC_SinglePartQuery);
		int _la;
		try {
			int _alt;
			setState(276);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,19,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(247);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << OPTIONAL) | (1L << MATCH) | (1L << UNWIND) | (1L << CALL))) != 0)) {
					{
					{
					setState(241);
					oC_ReadingClause();
					setState(243);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(242);
						match(SP);
						}
					}

					}
					}
					setState(249);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(250);
				oC_Return();
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(257);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << OPTIONAL) | (1L << MATCH) | (1L << UNWIND) | (1L << CALL))) != 0)) {
					{
					{
					setState(251);
					oC_ReadingClause();
					setState(253);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(252);
						match(SP);
						}
					}

					}
					}
					setState(259);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				setState(260);
				oC_UpdatingClause();
				setState(267);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,16,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(262);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(261);
							match(SP);
							}
						}

						setState(264);
						oC_UpdatingClause();
						}
						} 
					}
					setState(269);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,16,_ctx);
				}
				setState(274);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,18,_ctx) ) {
				case 1:
					{
					setState(271);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(270);
						match(SP);
						}
					}

					setState(273);
					oC_Return();
					}
					break;
				}
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_MultiPartQueryContext extends ParserRuleContext {
		public OC_SinglePartQueryContext oC_SinglePartQuery() {
			return getRuleContext(OC_SinglePartQueryContext.class,0);
		}
		public List<OC_WithContext> oC_With() {
			return getRuleContexts(OC_WithContext.class);
		}
		public OC_WithContext oC_With(int i) {
			return getRuleContext(OC_WithContext.class,i);
		}
		public List<OC_ReadingClauseContext> oC_ReadingClause() {
			return getRuleContexts(OC_ReadingClauseContext.class);
		}
		public OC_ReadingClauseContext oC_ReadingClause(int i) {
			return getRuleContext(OC_ReadingClauseContext.class,i);
		}
		public List<OC_UpdatingClauseContext> oC_UpdatingClause() {
			return getRuleContexts(OC_UpdatingClauseContext.class);
		}
		public OC_UpdatingClauseContext oC_UpdatingClause(int i) {
			return getRuleContext(OC_UpdatingClauseContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_MultiPartQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_MultiPartQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_MultiPartQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_MultiPartQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_MultiPartQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_MultiPartQueryContext oC_MultiPartQuery() throws RecognitionException {
		OC_MultiPartQueryContext _localctx = new OC_MultiPartQueryContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_oC_MultiPartQuery);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(300); 
			_errHandler.sync(this);
			_alt = 1;
			do {
				switch (_alt) {
				case 1:
					{
					{
					setState(284);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << OPTIONAL) | (1L << MATCH) | (1L << UNWIND) | (1L << CALL))) != 0)) {
						{
						{
						setState(278);
						oC_ReadingClause();
						setState(280);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(279);
							match(SP);
							}
						}

						}
						}
						setState(286);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(293);
					_errHandler.sync(this);
					_la = _input.LA(1);
					while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << MERGE) | (1L << CREATE) | (1L << SET) | (1L << DETACH) | (1L << DELETE) | (1L << REMOVE))) != 0)) {
						{
						{
						setState(287);
						oC_UpdatingClause();
						setState(289);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(288);
							match(SP);
							}
						}

						}
						}
						setState(295);
						_errHandler.sync(this);
						_la = _input.LA(1);
					}
					setState(296);
					oC_With();
					setState(298);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(297);
						match(SP);
						}
					}

					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(302); 
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,25,_ctx);
			} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
			setState(304);
			oC_SinglePartQuery();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_UpdatingClauseContext extends ParserRuleContext {
		public OC_CreateContext oC_Create() {
			return getRuleContext(OC_CreateContext.class,0);
		}
		public OC_MergeContext oC_Merge() {
			return getRuleContext(OC_MergeContext.class,0);
		}
		public OC_DeleteContext oC_Delete() {
			return getRuleContext(OC_DeleteContext.class,0);
		}
		public OC_SetContext oC_Set() {
			return getRuleContext(OC_SetContext.class,0);
		}
		public OC_RemoveContext oC_Remove() {
			return getRuleContext(OC_RemoveContext.class,0);
		}
		public OC_UpdatingClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_UpdatingClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_UpdatingClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_UpdatingClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_UpdatingClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_UpdatingClauseContext oC_UpdatingClause() throws RecognitionException {
		OC_UpdatingClauseContext _localctx = new OC_UpdatingClauseContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_oC_UpdatingClause);
		try {
			setState(311);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case CREATE:
				enterOuterAlt(_localctx, 1);
				{
				setState(306);
				oC_Create();
				}
				break;
			case MERGE:
				enterOuterAlt(_localctx, 2);
				{
				setState(307);
				oC_Merge();
				}
				break;
			case DETACH:
			case DELETE:
				enterOuterAlt(_localctx, 3);
				{
				setState(308);
				oC_Delete();
				}
				break;
			case SET:
				enterOuterAlt(_localctx, 4);
				{
				setState(309);
				oC_Set();
				}
				break;
			case REMOVE:
				enterOuterAlt(_localctx, 5);
				{
				setState(310);
				oC_Remove();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_ReadingClauseContext extends ParserRuleContext {
		public OC_MatchContext oC_Match() {
			return getRuleContext(OC_MatchContext.class,0);
		}
		public OC_UnwindContext oC_Unwind() {
			return getRuleContext(OC_UnwindContext.class,0);
		}
		public OC_InQueryCallContext oC_InQueryCall() {
			return getRuleContext(OC_InQueryCallContext.class,0);
		}
		public OC_ReadingClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_ReadingClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_ReadingClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_ReadingClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_ReadingClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_ReadingClauseContext oC_ReadingClause() throws RecognitionException {
		OC_ReadingClauseContext _localctx = new OC_ReadingClauseContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_oC_ReadingClause);
		try {
			setState(316);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case OPTIONAL:
			case MATCH:
				enterOuterAlt(_localctx, 1);
				{
				setState(313);
				oC_Match();
				}
				break;
			case UNWIND:
				enterOuterAlt(_localctx, 2);
				{
				setState(314);
				oC_Unwind();
				}
				break;
			case CALL:
				enterOuterAlt(_localctx, 3);
				{
				setState(315);
				oC_InQueryCall();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_MatchContext extends ParserRuleContext {
		public TerminalNode MATCH() { return getToken(CypherParser.MATCH, 0); }
		public OC_PatternContext oC_Pattern() {
			return getRuleContext(OC_PatternContext.class,0);
		}
		public TerminalNode OPTIONAL() { return getToken(CypherParser.OPTIONAL, 0); }
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_WhereContext oC_Where() {
			return getRuleContext(OC_WhereContext.class,0);
		}
		public OC_MatchContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Match; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Match(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Match(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Match(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_MatchContext oC_Match() throws RecognitionException {
		OC_MatchContext _localctx = new OC_MatchContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_oC_Match);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(320);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==OPTIONAL) {
				{
				setState(318);
				match(OPTIONAL);
				setState(319);
				match(SP);
				}
			}

			setState(322);
			match(MATCH);
			setState(324);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(323);
				match(SP);
				}
			}

			setState(326);
			oC_Pattern();
			setState(331);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,31,_ctx) ) {
			case 1:
				{
				setState(328);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(327);
					match(SP);
					}
				}

				setState(330);
				oC_Where();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_UnwindContext extends ParserRuleContext {
		public TerminalNode UNWIND() { return getToken(CypherParser.UNWIND, 0); }
		public OC_ExpressionContext oC_Expression() {
			return getRuleContext(OC_ExpressionContext.class,0);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public TerminalNode AS() { return getToken(CypherParser.AS, 0); }
		public OC_VariableContext oC_Variable() {
			return getRuleContext(OC_VariableContext.class,0);
		}
		public OC_UnwindContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Unwind; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Unwind(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Unwind(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Unwind(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_UnwindContext oC_Unwind() throws RecognitionException {
		OC_UnwindContext _localctx = new OC_UnwindContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_oC_Unwind);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(333);
			match(UNWIND);
			setState(335);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(334);
				match(SP);
				}
			}

			setState(337);
			oC_Expression();
			setState(338);
			match(SP);
			setState(339);
			match(AS);
			setState(340);
			match(SP);
			setState(341);
			oC_Variable();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_MergeContext extends ParserRuleContext {
		public TerminalNode MERGE() { return getToken(CypherParser.MERGE, 0); }
		public OC_PatternPartContext oC_PatternPart() {
			return getRuleContext(OC_PatternPartContext.class,0);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public List<OC_MergeActionContext> oC_MergeAction() {
			return getRuleContexts(OC_MergeActionContext.class);
		}
		public OC_MergeActionContext oC_MergeAction(int i) {
			return getRuleContext(OC_MergeActionContext.class,i);
		}
		public OC_MergeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Merge; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Merge(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Merge(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Merge(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_MergeContext oC_Merge() throws RecognitionException {
		OC_MergeContext _localctx = new OC_MergeContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_oC_Merge);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(343);
			match(MERGE);
			setState(345);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(344);
				match(SP);
				}
			}

			setState(347);
			oC_PatternPart();
			setState(352);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,34,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(348);
					match(SP);
					setState(349);
					oC_MergeAction();
					}
					} 
				}
				setState(354);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,34,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_MergeActionContext extends ParserRuleContext {
		public TerminalNode ON() { return getToken(CypherParser.ON, 0); }
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public TerminalNode MATCH() { return getToken(CypherParser.MATCH, 0); }
		public OC_SetContext oC_Set() {
			return getRuleContext(OC_SetContext.class,0);
		}
		public TerminalNode CREATE() { return getToken(CypherParser.CREATE, 0); }
		public OC_MergeActionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_MergeAction; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_MergeAction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_MergeAction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_MergeAction(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_MergeActionContext oC_MergeAction() throws RecognitionException {
		OC_MergeActionContext _localctx = new OC_MergeActionContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_oC_MergeAction);
		try {
			setState(365);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,35,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(355);
				match(ON);
				setState(356);
				match(SP);
				setState(357);
				match(MATCH);
				setState(358);
				match(SP);
				setState(359);
				oC_Set();
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(360);
				match(ON);
				setState(361);
				match(SP);
				setState(362);
				match(CREATE);
				setState(363);
				match(SP);
				setState(364);
				oC_Set();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_CreateContext extends ParserRuleContext {
		public TerminalNode CREATE() { return getToken(CypherParser.CREATE, 0); }
		public OC_PatternContext oC_Pattern() {
			return getRuleContext(OC_PatternContext.class,0);
		}
		public TerminalNode SP() { return getToken(CypherParser.SP, 0); }
		public OC_CreateContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Create; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Create(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Create(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Create(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_CreateContext oC_Create() throws RecognitionException {
		OC_CreateContext _localctx = new OC_CreateContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_oC_Create);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(367);
			match(CREATE);
			setState(369);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(368);
				match(SP);
				}
			}

			setState(371);
			oC_Pattern();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_SetContext extends ParserRuleContext {
		public TerminalNode SET() { return getToken(CypherParser.SET, 0); }
		public List<OC_SetItemContext> oC_SetItem() {
			return getRuleContexts(OC_SetItemContext.class);
		}
		public OC_SetItemContext oC_SetItem(int i) {
			return getRuleContext(OC_SetItemContext.class,i);
		}
		public TerminalNode SP() { return getToken(CypherParser.SP, 0); }
		public OC_SetContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Set; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Set(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Set(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Set(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_SetContext oC_Set() throws RecognitionException {
		OC_SetContext _localctx = new OC_SetContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_oC_Set);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(373);
			match(SET);
			setState(375);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(374);
				match(SP);
				}
			}

			setState(377);
			oC_SetItem();
			setState(382);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(378);
				match(T__1);
				setState(379);
				oC_SetItem();
				}
				}
				setState(384);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_SetItemContext extends ParserRuleContext {
		public OC_PropertyExpressionContext oC_PropertyExpression() {
			return getRuleContext(OC_PropertyExpressionContext.class,0);
		}
		public OC_ExpressionContext oC_Expression() {
			return getRuleContext(OC_ExpressionContext.class,0);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_VariableContext oC_Variable() {
			return getRuleContext(OC_VariableContext.class,0);
		}
		public OC_NodeLabelsContext oC_NodeLabels() {
			return getRuleContext(OC_NodeLabelsContext.class,0);
		}
		public OC_SetItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_SetItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_SetItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_SetItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_SetItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_SetItemContext oC_SetItem() throws RecognitionException {
		OC_SetItemContext _localctx = new OC_SetItemContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_oC_SetItem);
		int _la;
		try {
			setState(421);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,46,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(385);
				oC_PropertyExpression();
				setState(387);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(386);
					match(SP);
					}
				}

				setState(389);
				match(T__2);
				setState(391);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(390);
					match(SP);
					}
				}

				setState(393);
				oC_Expression();
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(395);
				oC_Variable();
				setState(397);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(396);
					match(SP);
					}
				}

				setState(399);
				match(T__2);
				setState(401);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(400);
					match(SP);
					}
				}

				setState(403);
				oC_Expression();
				}
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(405);
				oC_Variable();
				setState(407);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(406);
					match(SP);
					}
				}

				setState(409);
				match(T__3);
				setState(411);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(410);
					match(SP);
					}
				}

				setState(413);
				oC_Expression();
				}
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				{
				setState(415);
				oC_Variable();
				setState(417);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(416);
					match(SP);
					}
				}

				setState(419);
				oC_NodeLabels();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_DeleteContext extends ParserRuleContext {
		public TerminalNode DELETE() { return getToken(CypherParser.DELETE, 0); }
		public List<OC_ExpressionContext> oC_Expression() {
			return getRuleContexts(OC_ExpressionContext.class);
		}
		public OC_ExpressionContext oC_Expression(int i) {
			return getRuleContext(OC_ExpressionContext.class,i);
		}
		public TerminalNode DETACH() { return getToken(CypherParser.DETACH, 0); }
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_DeleteContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Delete; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Delete(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Delete(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Delete(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_DeleteContext oC_Delete() throws RecognitionException {
		OC_DeleteContext _localctx = new OC_DeleteContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_oC_Delete);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(425);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DETACH) {
				{
				setState(423);
				match(DETACH);
				setState(424);
				match(SP);
				}
			}

			setState(427);
			match(DELETE);
			setState(429);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(428);
				match(SP);
				}
			}

			setState(431);
			oC_Expression();
			setState(442);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,51,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(433);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(432);
						match(SP);
						}
					}

					setState(435);
					match(T__1);
					setState(437);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(436);
						match(SP);
						}
					}

					setState(439);
					oC_Expression();
					}
					} 
				}
				setState(444);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,51,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_RemoveContext extends ParserRuleContext {
		public TerminalNode REMOVE() { return getToken(CypherParser.REMOVE, 0); }
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public List<OC_RemoveItemContext> oC_RemoveItem() {
			return getRuleContexts(OC_RemoveItemContext.class);
		}
		public OC_RemoveItemContext oC_RemoveItem(int i) {
			return getRuleContext(OC_RemoveItemContext.class,i);
		}
		public OC_RemoveContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Remove; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Remove(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Remove(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Remove(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_RemoveContext oC_Remove() throws RecognitionException {
		OC_RemoveContext _localctx = new OC_RemoveContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_oC_Remove);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(445);
			match(REMOVE);
			setState(446);
			match(SP);
			setState(447);
			oC_RemoveItem();
			setState(458);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,54,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(449);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(448);
						match(SP);
						}
					}

					setState(451);
					match(T__1);
					setState(453);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(452);
						match(SP);
						}
					}

					setState(455);
					oC_RemoveItem();
					}
					} 
				}
				setState(460);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,54,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_RemoveItemContext extends ParserRuleContext {
		public OC_VariableContext oC_Variable() {
			return getRuleContext(OC_VariableContext.class,0);
		}
		public OC_NodeLabelsContext oC_NodeLabels() {
			return getRuleContext(OC_NodeLabelsContext.class,0);
		}
		public OC_PropertyExpressionContext oC_PropertyExpression() {
			return getRuleContext(OC_PropertyExpressionContext.class,0);
		}
		public OC_RemoveItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_RemoveItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_RemoveItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_RemoveItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_RemoveItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_RemoveItemContext oC_RemoveItem() throws RecognitionException {
		OC_RemoveItemContext _localctx = new OC_RemoveItemContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_oC_RemoveItem);
		try {
			setState(465);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,55,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(461);
				oC_Variable();
				setState(462);
				oC_NodeLabels();
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(464);
				oC_PropertyExpression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_InQueryCallContext extends ParserRuleContext {
		public TerminalNode CALL() { return getToken(CypherParser.CALL, 0); }
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_ExplicitProcedureInvocationContext oC_ExplicitProcedureInvocation() {
			return getRuleContext(OC_ExplicitProcedureInvocationContext.class,0);
		}
		public TerminalNode YIELD() { return getToken(CypherParser.YIELD, 0); }
		public OC_YieldItemsContext oC_YieldItems() {
			return getRuleContext(OC_YieldItemsContext.class,0);
		}
		public OC_InQueryCallContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_InQueryCall; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_InQueryCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_InQueryCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_InQueryCall(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_InQueryCallContext oC_InQueryCall() throws RecognitionException {
		OC_InQueryCallContext _localctx = new OC_InQueryCallContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_oC_InQueryCall);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(467);
			match(CALL);
			setState(468);
			match(SP);
			setState(469);
			oC_ExplicitProcedureInvocation();
			setState(476);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,57,_ctx) ) {
			case 1:
				{
				setState(471);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(470);
					match(SP);
					}
				}

				setState(473);
				match(YIELD);
				setState(474);
				match(SP);
				setState(475);
				oC_YieldItems();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_StandaloneCallContext extends ParserRuleContext {
		public TerminalNode CALL() { return getToken(CypherParser.CALL, 0); }
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_ExplicitProcedureInvocationContext oC_ExplicitProcedureInvocation() {
			return getRuleContext(OC_ExplicitProcedureInvocationContext.class,0);
		}
		public OC_ImplicitProcedureInvocationContext oC_ImplicitProcedureInvocation() {
			return getRuleContext(OC_ImplicitProcedureInvocationContext.class,0);
		}
		public TerminalNode YIELD() { return getToken(CypherParser.YIELD, 0); }
		public OC_YieldItemsContext oC_YieldItems() {
			return getRuleContext(OC_YieldItemsContext.class,0);
		}
		public OC_StandaloneCallContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_StandaloneCall; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_StandaloneCall(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_StandaloneCall(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_StandaloneCall(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_StandaloneCallContext oC_StandaloneCall() throws RecognitionException {
		OC_StandaloneCallContext _localctx = new OC_StandaloneCallContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_oC_StandaloneCall);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(478);
			match(CALL);
			setState(479);
			match(SP);
			setState(482);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,58,_ctx) ) {
			case 1:
				{
				setState(480);
				oC_ExplicitProcedureInvocation();
				}
				break;
			case 2:
				{
				setState(481);
				oC_ImplicitProcedureInvocation();
				}
				break;
			}
			setState(488);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,59,_ctx) ) {
			case 1:
				{
				setState(484);
				match(SP);
				setState(485);
				match(YIELD);
				setState(486);
				match(SP);
				setState(487);
				oC_YieldItems();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_YieldItemsContext extends ParserRuleContext {
		public List<OC_YieldItemContext> oC_YieldItem() {
			return getRuleContexts(OC_YieldItemContext.class);
		}
		public OC_YieldItemContext oC_YieldItem(int i) {
			return getRuleContext(OC_YieldItemContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_YieldItemsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_YieldItems; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_YieldItems(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_YieldItems(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_YieldItems(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_YieldItemsContext oC_YieldItems() throws RecognitionException {
		OC_YieldItemsContext _localctx = new OC_YieldItemsContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_oC_YieldItems);
		int _la;
		try {
			int _alt;
			setState(505);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case COUNT:
			case FILTER:
			case EXTRACT:
			case ANY:
			case NONE:
			case SINGLE:
			case HexLetter:
			case UnescapedSymbolicName:
			case EscapedSymbolicName:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(490);
				oC_YieldItem();
				setState(501);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,62,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(492);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(491);
							match(SP);
							}
						}

						setState(494);
						match(T__1);
						setState(496);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(495);
							match(SP);
							}
						}

						setState(498);
						oC_YieldItem();
						}
						} 
					}
					setState(503);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,62,_ctx);
				}
				}
				}
				break;
			case T__4:
				enterOuterAlt(_localctx, 2);
				{
				setState(504);
				match(T__4);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_YieldItemContext extends ParserRuleContext {
		public OC_VariableContext oC_Variable() {
			return getRuleContext(OC_VariableContext.class,0);
		}
		public OC_ProcedureResultFieldContext oC_ProcedureResultField() {
			return getRuleContext(OC_ProcedureResultFieldContext.class,0);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public TerminalNode AS() { return getToken(CypherParser.AS, 0); }
		public OC_YieldItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_YieldItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_YieldItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_YieldItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_YieldItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_YieldItemContext oC_YieldItem() throws RecognitionException {
		OC_YieldItemContext _localctx = new OC_YieldItemContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_oC_YieldItem);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(512);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,64,_ctx) ) {
			case 1:
				{
				setState(507);
				oC_ProcedureResultField();
				setState(508);
				match(SP);
				setState(509);
				match(AS);
				setState(510);
				match(SP);
				}
				break;
			}
			setState(514);
			oC_Variable();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_WithContext extends ParserRuleContext {
		public TerminalNode WITH() { return getToken(CypherParser.WITH, 0); }
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_ReturnBodyContext oC_ReturnBody() {
			return getRuleContext(OC_ReturnBodyContext.class,0);
		}
		public TerminalNode DISTINCT() { return getToken(CypherParser.DISTINCT, 0); }
		public OC_WhereContext oC_Where() {
			return getRuleContext(OC_WhereContext.class,0);
		}
		public OC_WithContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_With; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_With(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_With(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_With(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_WithContext oC_With() throws RecognitionException {
		OC_WithContext _localctx = new OC_WithContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_oC_With);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(516);
			match(WITH);
			setState(521);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,66,_ctx) ) {
			case 1:
				{
				setState(518);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(517);
					match(SP);
					}
				}

				setState(520);
				match(DISTINCT);
				}
				break;
			}
			setState(523);
			match(SP);
			setState(524);
			oC_ReturnBody();
			setState(529);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,68,_ctx) ) {
			case 1:
				{
				setState(526);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(525);
					match(SP);
					}
				}

				setState(528);
				oC_Where();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_ReturnContext extends ParserRuleContext {
		public TerminalNode RETURN() { return getToken(CypherParser.RETURN, 0); }
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_ReturnBodyContext oC_ReturnBody() {
			return getRuleContext(OC_ReturnBodyContext.class,0);
		}
		public TerminalNode DISTINCT() { return getToken(CypherParser.DISTINCT, 0); }
		public OC_ReturnContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Return; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Return(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Return(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Return(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_ReturnContext oC_Return() throws RecognitionException {
		OC_ReturnContext _localctx = new OC_ReturnContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_oC_Return);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(531);
			match(RETURN);
			setState(536);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,70,_ctx) ) {
			case 1:
				{
				setState(533);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(532);
					match(SP);
					}
				}

				setState(535);
				match(DISTINCT);
				}
				break;
			}
			setState(538);
			match(SP);
			setState(539);
			oC_ReturnBody();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_ReturnBodyContext extends ParserRuleContext {
		public OC_ReturnItemsContext oC_ReturnItems() {
			return getRuleContext(OC_ReturnItemsContext.class,0);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_OrderContext oC_Order() {
			return getRuleContext(OC_OrderContext.class,0);
		}
		public OC_SkipContext oC_Skip() {
			return getRuleContext(OC_SkipContext.class,0);
		}
		public OC_LimitContext oC_Limit() {
			return getRuleContext(OC_LimitContext.class,0);
		}
		public OC_ReturnBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_ReturnBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_ReturnBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_ReturnBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_ReturnBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_ReturnBodyContext oC_ReturnBody() throws RecognitionException {
		OC_ReturnBodyContext _localctx = new OC_ReturnBodyContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_oC_ReturnBody);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(541);
			oC_ReturnItems();
			setState(544);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,71,_ctx) ) {
			case 1:
				{
				setState(542);
				match(SP);
				setState(543);
				oC_Order();
				}
				break;
			}
			setState(548);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,72,_ctx) ) {
			case 1:
				{
				setState(546);
				match(SP);
				setState(547);
				oC_Skip();
				}
				break;
			}
			setState(552);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,73,_ctx) ) {
			case 1:
				{
				setState(550);
				match(SP);
				setState(551);
				oC_Limit();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_ReturnItemsContext extends ParserRuleContext {
		public List<OC_ReturnItemContext> oC_ReturnItem() {
			return getRuleContexts(OC_ReturnItemContext.class);
		}
		public OC_ReturnItemContext oC_ReturnItem(int i) {
			return getRuleContext(OC_ReturnItemContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_ReturnItemsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_ReturnItems; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_ReturnItems(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_ReturnItems(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_ReturnItems(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_ReturnItemsContext oC_ReturnItems() throws RecognitionException {
		OC_ReturnItemsContext _localctx = new OC_ReturnItemsContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_oC_ReturnItems);
		int _la;
		try {
			int _alt;
			setState(582);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__5:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(554);
				match(T__5);
				setState(565);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,76,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(556);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(555);
							match(SP);
							}
						}

						setState(558);
						match(T__1);
						setState(560);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(559);
							match(SP);
							}
						}

						setState(562);
						oC_ReturnItem();
						}
						} 
					}
					setState(567);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,76,_ctx);
				}
				}
				}
				break;
			case T__4:
			case T__6:
			case T__8:
			case T__13:
			case T__23:
			case T__25:
			case ALL:
			case NOT:
			case NULL:
			case COUNT:
			case FILTER:
			case EXTRACT:
			case ANY:
			case NONE:
			case SINGLE:
			case TRUE:
			case FALSE:
			case EXISTS:
			case CASE:
			case StringLiteral:
			case HexInteger:
			case DecimalInteger:
			case OctalInteger:
			case HexLetter:
			case ExponentDecimalReal:
			case RegularDecimalReal:
			case UnescapedSymbolicName:
			case EscapedSymbolicName:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(568);
				oC_ReturnItem();
				setState(579);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,79,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(570);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(569);
							match(SP);
							}
						}

						setState(572);
						match(T__1);
						setState(574);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(573);
							match(SP);
							}
						}

						setState(576);
						oC_ReturnItem();
						}
						} 
					}
					setState(581);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,79,_ctx);
				}
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_ReturnItemContext extends ParserRuleContext {
		public OC_ExpressionContext oC_Expression() {
			return getRuleContext(OC_ExpressionContext.class,0);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public TerminalNode AS() { return getToken(CypherParser.AS, 0); }
		public OC_VariableContext oC_Variable() {
			return getRuleContext(OC_VariableContext.class,0);
		}
		public OC_ReturnItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_ReturnItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_ReturnItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_ReturnItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_ReturnItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_ReturnItemContext oC_ReturnItem() throws RecognitionException {
		OC_ReturnItemContext _localctx = new OC_ReturnItemContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_oC_ReturnItem);
		try {
			setState(591);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,81,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(584);
				oC_Expression();
				setState(585);
				match(SP);
				setState(586);
				match(AS);
				setState(587);
				match(SP);
				setState(588);
				oC_Variable();
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(590);
				oC_Expression();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_OrderContext extends ParserRuleContext {
		public TerminalNode ORDER() { return getToken(CypherParser.ORDER, 0); }
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public TerminalNode BY() { return getToken(CypherParser.BY, 0); }
		public List<OC_SortItemContext> oC_SortItem() {
			return getRuleContexts(OC_SortItemContext.class);
		}
		public OC_SortItemContext oC_SortItem(int i) {
			return getRuleContext(OC_SortItemContext.class,i);
		}
		public OC_OrderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Order; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Order(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Order(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Order(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_OrderContext oC_Order() throws RecognitionException {
		OC_OrderContext _localctx = new OC_OrderContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_oC_Order);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(593);
			match(ORDER);
			setState(594);
			match(SP);
			setState(595);
			match(BY);
			setState(596);
			match(SP);
			setState(597);
			oC_SortItem();
			setState(605);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__1) {
				{
				{
				setState(598);
				match(T__1);
				setState(600);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(599);
					match(SP);
					}
				}

				setState(602);
				oC_SortItem();
				}
				}
				setState(607);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_SkipContext extends ParserRuleContext {
		public TerminalNode L_SKIP() { return getToken(CypherParser.L_SKIP, 0); }
		public TerminalNode SP() { return getToken(CypherParser.SP, 0); }
		public OC_ExpressionContext oC_Expression() {
			return getRuleContext(OC_ExpressionContext.class,0);
		}
		public OC_SkipContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Skip; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Skip(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Skip(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Skip(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_SkipContext oC_Skip() throws RecognitionException {
		OC_SkipContext _localctx = new OC_SkipContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_oC_Skip);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(608);
			match(L_SKIP);
			setState(609);
			match(SP);
			setState(610);
			oC_Expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_LimitContext extends ParserRuleContext {
		public TerminalNode LIMIT() { return getToken(CypherParser.LIMIT, 0); }
		public TerminalNode SP() { return getToken(CypherParser.SP, 0); }
		public OC_ExpressionContext oC_Expression() {
			return getRuleContext(OC_ExpressionContext.class,0);
		}
		public OC_LimitContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Limit; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Limit(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Limit(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Limit(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_LimitContext oC_Limit() throws RecognitionException {
		OC_LimitContext _localctx = new OC_LimitContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_oC_Limit);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(612);
			match(LIMIT);
			setState(613);
			match(SP);
			setState(614);
			oC_Expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_SortItemContext extends ParserRuleContext {
		public OC_ExpressionContext oC_Expression() {
			return getRuleContext(OC_ExpressionContext.class,0);
		}
		public TerminalNode ASCENDING() { return getToken(CypherParser.ASCENDING, 0); }
		public TerminalNode ASC() { return getToken(CypherParser.ASC, 0); }
		public TerminalNode DESCENDING() { return getToken(CypherParser.DESCENDING, 0); }
		public TerminalNode DESC() { return getToken(CypherParser.DESC, 0); }
		public TerminalNode SP() { return getToken(CypherParser.SP, 0); }
		public OC_SortItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_SortItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_SortItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_SortItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_SortItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_SortItemContext oC_SortItem() throws RecognitionException {
		OC_SortItemContext _localctx = new OC_SortItemContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_oC_SortItem);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(616);
			oC_Expression();
			setState(621);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,85,_ctx) ) {
			case 1:
				{
				setState(618);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(617);
					match(SP);
					}
				}

				setState(620);
				_la = _input.LA(1);
				if ( !(((((_la - 68)) & ~0x3f) == 0 && ((1L << (_la - 68)) & ((1L << (ASCENDING - 68)) | (1L << (ASC - 68)) | (1L << (DESCENDING - 68)) | (1L << (DESC - 68)))) != 0)) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_WhereContext extends ParserRuleContext {
		public TerminalNode WHERE() { return getToken(CypherParser.WHERE, 0); }
		public TerminalNode SP() { return getToken(CypherParser.SP, 0); }
		public OC_ExpressionContext oC_Expression() {
			return getRuleContext(OC_ExpressionContext.class,0);
		}
		public OC_WhereContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Where; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Where(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Where(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Where(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_WhereContext oC_Where() throws RecognitionException {
		OC_WhereContext _localctx = new OC_WhereContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_oC_Where);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(623);
			match(WHERE);
			setState(624);
			match(SP);
			setState(625);
			oC_Expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_PatternContext extends ParserRuleContext {
		public List<OC_PatternPartContext> oC_PatternPart() {
			return getRuleContexts(OC_PatternPartContext.class);
		}
		public OC_PatternPartContext oC_PatternPart(int i) {
			return getRuleContext(OC_PatternPartContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_PatternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Pattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Pattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Pattern(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Pattern(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_PatternContext oC_Pattern() throws RecognitionException {
		OC_PatternContext _localctx = new OC_PatternContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_oC_Pattern);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(627);
			oC_PatternPart();
			setState(638);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,88,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(629);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(628);
						match(SP);
						}
					}

					setState(631);
					match(T__1);
					setState(633);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(632);
						match(SP);
						}
					}

					setState(635);
					oC_PatternPart();
					}
					} 
				}
				setState(640);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,88,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_PatternPartContext extends ParserRuleContext {
		public OC_VariableContext oC_Variable() {
			return getRuleContext(OC_VariableContext.class,0);
		}
		public OC_AnonymousPatternPartContext oC_AnonymousPatternPart() {
			return getRuleContext(OC_AnonymousPatternPartContext.class,0);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_PatternPartContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_PatternPart; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_PatternPart(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_PatternPart(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_PatternPart(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_PatternPartContext oC_PatternPart() throws RecognitionException {
		OC_PatternPartContext _localctx = new OC_PatternPartContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_oC_PatternPart);
		int _la;
		try {
			setState(652);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case COUNT:
			case FILTER:
			case EXTRACT:
			case ANY:
			case NONE:
			case SINGLE:
			case HexLetter:
			case UnescapedSymbolicName:
			case EscapedSymbolicName:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(641);
				oC_Variable();
				setState(643);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(642);
					match(SP);
					}
				}

				setState(645);
				match(T__2);
				setState(647);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(646);
					match(SP);
					}
				}

				setState(649);
				oC_AnonymousPatternPart();
				}
				}
				break;
			case T__6:
				enterOuterAlt(_localctx, 2);
				{
				setState(651);
				oC_AnonymousPatternPart();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_AnonymousPatternPartContext extends ParserRuleContext {
		public OC_PatternElementContext oC_PatternElement() {
			return getRuleContext(OC_PatternElementContext.class,0);
		}
		public OC_AnonymousPatternPartContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_AnonymousPatternPart; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_AnonymousPatternPart(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_AnonymousPatternPart(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_AnonymousPatternPart(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_AnonymousPatternPartContext oC_AnonymousPatternPart() throws RecognitionException {
		OC_AnonymousPatternPartContext _localctx = new OC_AnonymousPatternPartContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_oC_AnonymousPatternPart);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(654);
			oC_PatternElement();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_PatternElementContext extends ParserRuleContext {
		public OC_NodePatternContext oC_NodePattern() {
			return getRuleContext(OC_NodePatternContext.class,0);
		}
		public List<OC_PatternElementChainContext> oC_PatternElementChain() {
			return getRuleContexts(OC_PatternElementChainContext.class);
		}
		public OC_PatternElementChainContext oC_PatternElementChain(int i) {
			return getRuleContext(OC_PatternElementChainContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_PatternElementContext oC_PatternElement() {
			return getRuleContext(OC_PatternElementContext.class,0);
		}
		public OC_PatternElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_PatternElement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_PatternElement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_PatternElement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_PatternElement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_PatternElementContext oC_PatternElement() throws RecognitionException {
		OC_PatternElementContext _localctx = new OC_PatternElementContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_oC_PatternElement);
		int _la;
		try {
			int _alt;
			setState(670);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,94,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(656);
				oC_NodePattern();
				setState(663);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,93,_ctx);
				while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
					if ( _alt==1 ) {
						{
						{
						setState(658);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(657);
							match(SP);
							}
						}

						setState(660);
						oC_PatternElementChain();
						}
						} 
					}
					setState(665);
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,93,_ctx);
				}
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(666);
				match(T__6);
				setState(667);
				oC_PatternElement();
				setState(668);
				match(T__7);
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_NodePatternContext extends ParserRuleContext {
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_VariableContext oC_Variable() {
			return getRuleContext(OC_VariableContext.class,0);
		}
		public OC_NodeLabelsContext oC_NodeLabels() {
			return getRuleContext(OC_NodeLabelsContext.class,0);
		}
		public OC_PropertiesContext oC_Properties() {
			return getRuleContext(OC_PropertiesContext.class,0);
		}
		public OC_NodePatternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_NodePattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_NodePattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_NodePattern(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_NodePattern(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_NodePatternContext oC_NodePattern() throws RecognitionException {
		OC_NodePatternContext _localctx = new OC_NodePatternContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_oC_NodePattern);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(672);
			match(T__6);
			setState(674);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(673);
				match(SP);
				}
			}

			setState(680);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 83)) & ~0x3f) == 0 && ((1L << (_la - 83)) & ((1L << (COUNT - 83)) | (1L << (FILTER - 83)) | (1L << (EXTRACT - 83)) | (1L << (ANY - 83)) | (1L << (NONE - 83)) | (1L << (SINGLE - 83)) | (1L << (HexLetter - 83)) | (1L << (UnescapedSymbolicName - 83)) | (1L << (EscapedSymbolicName - 83)))) != 0)) {
				{
				setState(676);
				oC_Variable();
				setState(678);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(677);
					match(SP);
					}
				}

				}
			}

			setState(686);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__10) {
				{
				setState(682);
				oC_NodeLabels();
				setState(684);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(683);
					match(SP);
					}
				}

				}
			}

			setState(692);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__23 || _la==T__25) {
				{
				setState(688);
				oC_Properties();
				setState(690);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(689);
					match(SP);
					}
				}

				}
			}

			setState(694);
			match(T__7);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_PatternElementChainContext extends ParserRuleContext {
		public OC_RelationshipPatternContext oC_RelationshipPattern() {
			return getRuleContext(OC_RelationshipPatternContext.class,0);
		}
		public OC_NodePatternContext oC_NodePattern() {
			return getRuleContext(OC_NodePatternContext.class,0);
		}
		public TerminalNode SP() { return getToken(CypherParser.SP, 0); }
		public OC_PatternElementChainContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_PatternElementChain; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_PatternElementChain(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_PatternElementChain(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_PatternElementChain(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_PatternElementChainContext oC_PatternElementChain() throws RecognitionException {
		OC_PatternElementChainContext _localctx = new OC_PatternElementChainContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_oC_PatternElementChain);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(696);
			oC_RelationshipPattern();
			setState(698);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(697);
				match(SP);
				}
			}

			setState(700);
			oC_NodePattern();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_RelationshipPatternContext extends ParserRuleContext {
		public OC_LeftArrowHeadContext oC_LeftArrowHead() {
			return getRuleContext(OC_LeftArrowHeadContext.class,0);
		}
		public List<OC_DashContext> oC_Dash() {
			return getRuleContexts(OC_DashContext.class);
		}
		public OC_DashContext oC_Dash(int i) {
			return getRuleContext(OC_DashContext.class,i);
		}
		public OC_RightArrowHeadContext oC_RightArrowHead() {
			return getRuleContext(OC_RightArrowHeadContext.class,0);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_RelationshipDetailContext oC_RelationshipDetail() {
			return getRuleContext(OC_RelationshipDetailContext.class,0);
		}
		public OC_RelationshipPatternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_RelationshipPattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_RelationshipPattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_RelationshipPattern(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_RelationshipPattern(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_RelationshipPatternContext oC_RelationshipPattern() throws RecognitionException {
		OC_RelationshipPatternContext _localctx = new OC_RelationshipPatternContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_oC_RelationshipPattern);
		int _la;
		try {
			setState(766);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,119,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(702);
				oC_LeftArrowHead();
				setState(704);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(703);
					match(SP);
					}
				}

				setState(706);
				oC_Dash();
				setState(708);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,104,_ctx) ) {
				case 1:
					{
					setState(707);
					match(SP);
					}
					break;
				}
				setState(711);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__8) {
					{
					setState(710);
					oC_RelationshipDetail();
					}
				}

				setState(714);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(713);
					match(SP);
					}
				}

				setState(716);
				oC_Dash();
				setState(718);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(717);
					match(SP);
					}
				}

				setState(720);
				oC_RightArrowHead();
				}
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(722);
				oC_LeftArrowHead();
				setState(724);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(723);
					match(SP);
					}
				}

				setState(726);
				oC_Dash();
				setState(728);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,109,_ctx) ) {
				case 1:
					{
					setState(727);
					match(SP);
					}
					break;
				}
				setState(731);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__8) {
					{
					setState(730);
					oC_RelationshipDetail();
					}
				}

				setState(734);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(733);
					match(SP);
					}
				}

				setState(736);
				oC_Dash();
				}
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(738);
				oC_Dash();
				setState(740);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,112,_ctx) ) {
				case 1:
					{
					setState(739);
					match(SP);
					}
					break;
				}
				setState(743);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__8) {
					{
					setState(742);
					oC_RelationshipDetail();
					}
				}

				setState(746);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(745);
					match(SP);
					}
				}

				setState(748);
				oC_Dash();
				setState(750);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(749);
					match(SP);
					}
				}

				setState(752);
				oC_RightArrowHead();
				}
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				{
				setState(754);
				oC_Dash();
				setState(756);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,116,_ctx) ) {
				case 1:
					{
					setState(755);
					match(SP);
					}
					break;
				}
				setState(759);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__8) {
					{
					setState(758);
					oC_RelationshipDetail();
					}
				}

				setState(762);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(761);
					match(SP);
					}
				}

				setState(764);
				oC_Dash();
				}
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_RelationshipDetailContext extends ParserRuleContext {
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_VariableContext oC_Variable() {
			return getRuleContext(OC_VariableContext.class,0);
		}
		public OC_RelationshipTypesContext oC_RelationshipTypes() {
			return getRuleContext(OC_RelationshipTypesContext.class,0);
		}
		public OC_RangeLiteralContext oC_RangeLiteral() {
			return getRuleContext(OC_RangeLiteralContext.class,0);
		}
		public OC_PropertiesContext oC_Properties() {
			return getRuleContext(OC_PropertiesContext.class,0);
		}
		public OC_RelationshipDetailContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_RelationshipDetail; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_RelationshipDetail(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_RelationshipDetail(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_RelationshipDetail(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_RelationshipDetailContext oC_RelationshipDetail() throws RecognitionException {
		OC_RelationshipDetailContext _localctx = new OC_RelationshipDetailContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_oC_RelationshipDetail);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(768);
			match(T__8);
			setState(770);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(769);
				match(SP);
				}
			}

			setState(776);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 83)) & ~0x3f) == 0 && ((1L << (_la - 83)) & ((1L << (COUNT - 83)) | (1L << (FILTER - 83)) | (1L << (EXTRACT - 83)) | (1L << (ANY - 83)) | (1L << (NONE - 83)) | (1L << (SINGLE - 83)) | (1L << (HexLetter - 83)) | (1L << (UnescapedSymbolicName - 83)) | (1L << (EscapedSymbolicName - 83)))) != 0)) {
				{
				setState(772);
				oC_Variable();
				setState(774);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(773);
					match(SP);
					}
				}

				}
			}

			setState(782);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__10) {
				{
				setState(778);
				oC_RelationshipTypes();
				setState(780);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(779);
					match(SP);
					}
				}

				}
			}

			setState(785);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__5) {
				{
				setState(784);
				oC_RangeLiteral();
				}
			}

			setState(791);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__23 || _la==T__25) {
				{
				setState(787);
				oC_Properties();
				setState(789);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(788);
					match(SP);
					}
				}

				}
			}

			setState(793);
			match(T__9);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_PropertiesContext extends ParserRuleContext {
		public OC_MapLiteralContext oC_MapLiteral() {
			return getRuleContext(OC_MapLiteralContext.class,0);
		}
		public OC_ParameterContext oC_Parameter() {
			return getRuleContext(OC_ParameterContext.class,0);
		}
		public OC_PropertiesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Properties; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Properties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Properties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Properties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_PropertiesContext oC_Properties() throws RecognitionException {
		OC_PropertiesContext _localctx = new OC_PropertiesContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_oC_Properties);
		try {
			setState(797);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__23:
				enterOuterAlt(_localctx, 1);
				{
				setState(795);
				oC_MapLiteral();
				}
				break;
			case T__25:
				enterOuterAlt(_localctx, 2);
				{
				setState(796);
				oC_Parameter();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_RelationshipTypesContext extends ParserRuleContext {
		public List<OC_RelTypeNameContext> oC_RelTypeName() {
			return getRuleContexts(OC_RelTypeNameContext.class);
		}
		public OC_RelTypeNameContext oC_RelTypeName(int i) {
			return getRuleContext(OC_RelTypeNameContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_RelationshipTypesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_RelationshipTypes; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_RelationshipTypes(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_RelationshipTypes(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_RelationshipTypes(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_RelationshipTypesContext oC_RelationshipTypes() throws RecognitionException {
		OC_RelationshipTypesContext _localctx = new OC_RelationshipTypesContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_oC_RelationshipTypes);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(799);
			match(T__10);
			setState(801);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(800);
				match(SP);
				}
			}

			setState(803);
			oC_RelTypeName();
			setState(817);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,133,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(805);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(804);
						match(SP);
						}
					}

					setState(807);
					match(T__11);
					setState(809);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==T__10) {
						{
						setState(808);
						match(T__10);
						}
					}

					setState(812);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(811);
						match(SP);
						}
					}

					setState(814);
					oC_RelTypeName();
					}
					} 
				}
				setState(819);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,133,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_NodeLabelsContext extends ParserRuleContext {
		public List<OC_NodeLabelContext> oC_NodeLabel() {
			return getRuleContexts(OC_NodeLabelContext.class);
		}
		public OC_NodeLabelContext oC_NodeLabel(int i) {
			return getRuleContext(OC_NodeLabelContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_NodeLabelsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_NodeLabels; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_NodeLabels(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_NodeLabels(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_NodeLabels(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_NodeLabelsContext oC_NodeLabels() throws RecognitionException {
		OC_NodeLabelsContext _localctx = new OC_NodeLabelsContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_oC_NodeLabels);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(820);
			oC_NodeLabel();
			setState(827);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,135,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(822);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(821);
						match(SP);
						}
					}

					setState(824);
					oC_NodeLabel();
					}
					} 
				}
				setState(829);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,135,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_NodeLabelContext extends ParserRuleContext {
		public OC_LabelNameContext oC_LabelName() {
			return getRuleContext(OC_LabelNameContext.class,0);
		}
		public TerminalNode SP() { return getToken(CypherParser.SP, 0); }
		public OC_NodeLabelContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_NodeLabel; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_NodeLabel(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_NodeLabel(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_NodeLabel(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_NodeLabelContext oC_NodeLabel() throws RecognitionException {
		OC_NodeLabelContext _localctx = new OC_NodeLabelContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_oC_NodeLabel);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(830);
			match(T__10);
			setState(832);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(831);
				match(SP);
				}
			}

			setState(834);
			oC_LabelName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_RangeLiteralContext extends ParserRuleContext {
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public List<OC_IntegerLiteralContext> oC_IntegerLiteral() {
			return getRuleContexts(OC_IntegerLiteralContext.class);
		}
		public OC_IntegerLiteralContext oC_IntegerLiteral(int i) {
			return getRuleContext(OC_IntegerLiteralContext.class,i);
		}
		public OC_RangeLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_RangeLiteral; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_RangeLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_RangeLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_RangeLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_RangeLiteralContext oC_RangeLiteral() throws RecognitionException {
		OC_RangeLiteralContext _localctx = new OC_RangeLiteralContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_oC_RangeLiteral);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(836);
			match(T__5);
			setState(838);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(837);
				match(SP);
				}
			}

			setState(844);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 99)) & ~0x3f) == 0 && ((1L << (_la - 99)) & ((1L << (HexInteger - 99)) | (1L << (DecimalInteger - 99)) | (1L << (OctalInteger - 99)))) != 0)) {
				{
				setState(840);
				oC_IntegerLiteral();
				setState(842);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(841);
					match(SP);
					}
				}

				}
			}

			setState(856);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==T__12) {
				{
				setState(846);
				match(T__12);
				setState(848);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(847);
					match(SP);
					}
				}

				setState(854);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (((((_la - 99)) & ~0x3f) == 0 && ((1L << (_la - 99)) & ((1L << (HexInteger - 99)) | (1L << (DecimalInteger - 99)) | (1L << (OctalInteger - 99)))) != 0)) {
					{
					setState(850);
					oC_IntegerLiteral();
					setState(852);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(851);
						match(SP);
						}
					}

					}
				}

				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_LabelNameContext extends ParserRuleContext {
		public OC_SchemaNameContext oC_SchemaName() {
			return getRuleContext(OC_SchemaNameContext.class,0);
		}
		public OC_LabelNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_LabelName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_LabelName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_LabelName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_LabelName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_LabelNameContext oC_LabelName() throws RecognitionException {
		OC_LabelNameContext _localctx = new OC_LabelNameContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_oC_LabelName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(858);
			oC_SchemaName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_RelTypeNameContext extends ParserRuleContext {
		public OC_SchemaNameContext oC_SchemaName() {
			return getRuleContext(OC_SchemaNameContext.class,0);
		}
		public OC_RelTypeNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_RelTypeName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_RelTypeName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_RelTypeName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_RelTypeName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_RelTypeNameContext oC_RelTypeName() throws RecognitionException {
		OC_RelTypeNameContext _localctx = new OC_RelTypeNameContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_oC_RelTypeName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(860);
			oC_SchemaName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_ExpressionContext extends ParserRuleContext {
		public OC_OrExpressionContext oC_OrExpression() {
			return getRuleContext(OC_OrExpressionContext.class,0);
		}
		public OC_ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Expression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Expression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Expression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_ExpressionContext oC_Expression() throws RecognitionException {
		OC_ExpressionContext _localctx = new OC_ExpressionContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_oC_Expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(862);
			oC_OrExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_OrExpressionContext extends ParserRuleContext {
		public List<OC_XorExpressionContext> oC_XorExpression() {
			return getRuleContexts(OC_XorExpressionContext.class);
		}
		public OC_XorExpressionContext oC_XorExpression(int i) {
			return getRuleContext(OC_XorExpressionContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public List<TerminalNode> OR() { return getTokens(CypherParser.OR); }
		public TerminalNode OR(int i) {
			return getToken(CypherParser.OR, i);
		}
		public OC_OrExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_OrExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_OrExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_OrExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_OrExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_OrExpressionContext oC_OrExpression() throws RecognitionException {
		OC_OrExpressionContext _localctx = new OC_OrExpressionContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_oC_OrExpression);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(864);
			oC_XorExpression();
			setState(871);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,144,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(865);
					match(SP);
					setState(866);
					match(OR);
					setState(867);
					match(SP);
					setState(868);
					oC_XorExpression();
					}
					} 
				}
				setState(873);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,144,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_XorExpressionContext extends ParserRuleContext {
		public List<OC_AndExpressionContext> oC_AndExpression() {
			return getRuleContexts(OC_AndExpressionContext.class);
		}
		public OC_AndExpressionContext oC_AndExpression(int i) {
			return getRuleContext(OC_AndExpressionContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public List<TerminalNode> XOR() { return getTokens(CypherParser.XOR); }
		public TerminalNode XOR(int i) {
			return getToken(CypherParser.XOR, i);
		}
		public OC_XorExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_XorExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_XorExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_XorExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_XorExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_XorExpressionContext oC_XorExpression() throws RecognitionException {
		OC_XorExpressionContext _localctx = new OC_XorExpressionContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_oC_XorExpression);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(874);
			oC_AndExpression();
			setState(881);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,145,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(875);
					match(SP);
					setState(876);
					match(XOR);
					setState(877);
					match(SP);
					setState(878);
					oC_AndExpression();
					}
					} 
				}
				setState(883);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,145,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_AndExpressionContext extends ParserRuleContext {
		public List<OC_NotExpressionContext> oC_NotExpression() {
			return getRuleContexts(OC_NotExpressionContext.class);
		}
		public OC_NotExpressionContext oC_NotExpression(int i) {
			return getRuleContext(OC_NotExpressionContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public List<TerminalNode> AND() { return getTokens(CypherParser.AND); }
		public TerminalNode AND(int i) {
			return getToken(CypherParser.AND, i);
		}
		public OC_AndExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_AndExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_AndExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_AndExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_AndExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_AndExpressionContext oC_AndExpression() throws RecognitionException {
		OC_AndExpressionContext _localctx = new OC_AndExpressionContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_oC_AndExpression);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(884);
			oC_NotExpression();
			setState(891);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,146,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(885);
					match(SP);
					setState(886);
					match(AND);
					setState(887);
					match(SP);
					setState(888);
					oC_NotExpression();
					}
					} 
				}
				setState(893);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,146,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_NotExpressionContext extends ParserRuleContext {
		public OC_ComparisonExpressionContext oC_ComparisonExpression() {
			return getRuleContext(OC_ComparisonExpressionContext.class,0);
		}
		public List<TerminalNode> NOT() { return getTokens(CypherParser.NOT); }
		public TerminalNode NOT(int i) {
			return getToken(CypherParser.NOT, i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_NotExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_NotExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_NotExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_NotExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_NotExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_NotExpressionContext oC_NotExpression() throws RecognitionException {
		OC_NotExpressionContext _localctx = new OC_NotExpressionContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_oC_NotExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(900);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==NOT) {
				{
				{
				setState(894);
				match(NOT);
				setState(896);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(895);
					match(SP);
					}
				}

				}
				}
				setState(902);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(903);
			oC_ComparisonExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_ComparisonExpressionContext extends ParserRuleContext {
		public OC_AddOrSubtractExpressionContext oC_AddOrSubtractExpression() {
			return getRuleContext(OC_AddOrSubtractExpressionContext.class,0);
		}
		public List<OC_PartialComparisonExpressionContext> oC_PartialComparisonExpression() {
			return getRuleContexts(OC_PartialComparisonExpressionContext.class);
		}
		public OC_PartialComparisonExpressionContext oC_PartialComparisonExpression(int i) {
			return getRuleContext(OC_PartialComparisonExpressionContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_ComparisonExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_ComparisonExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_ComparisonExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_ComparisonExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_ComparisonExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_ComparisonExpressionContext oC_ComparisonExpression() throws RecognitionException {
		OC_ComparisonExpressionContext _localctx = new OC_ComparisonExpressionContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_oC_ComparisonExpression);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(905);
			oC_AddOrSubtractExpression();
			setState(912);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,150,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(907);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(906);
						match(SP);
						}
					}

					setState(909);
					oC_PartialComparisonExpression();
					}
					} 
				}
				setState(914);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,150,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_AddOrSubtractExpressionContext extends ParserRuleContext {
		public List<OC_MultiplyDivideModuloExpressionContext> oC_MultiplyDivideModuloExpression() {
			return getRuleContexts(OC_MultiplyDivideModuloExpressionContext.class);
		}
		public OC_MultiplyDivideModuloExpressionContext oC_MultiplyDivideModuloExpression(int i) {
			return getRuleContext(OC_MultiplyDivideModuloExpressionContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_AddOrSubtractExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_AddOrSubtractExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_AddOrSubtractExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_AddOrSubtractExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_AddOrSubtractExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_AddOrSubtractExpressionContext oC_AddOrSubtractExpression() throws RecognitionException {
		OC_AddOrSubtractExpressionContext _localctx = new OC_AddOrSubtractExpressionContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_oC_AddOrSubtractExpression);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(915);
			oC_MultiplyDivideModuloExpression();
			setState(934);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,156,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					setState(932);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,155,_ctx) ) {
					case 1:
						{
						{
						setState(917);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(916);
							match(SP);
							}
						}

						setState(919);
						match(T__13);
						setState(921);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(920);
							match(SP);
							}
						}

						setState(923);
						oC_MultiplyDivideModuloExpression();
						}
						}
						break;
					case 2:
						{
						{
						setState(925);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(924);
							match(SP);
							}
						}

						setState(927);
						match(T__4);
						setState(929);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(928);
							match(SP);
							}
						}

						setState(931);
						oC_MultiplyDivideModuloExpression();
						}
						}
						break;
					}
					} 
				}
				setState(936);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,156,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_MultiplyDivideModuloExpressionContext extends ParserRuleContext {
		public List<OC_PowerOfExpressionContext> oC_PowerOfExpression() {
			return getRuleContexts(OC_PowerOfExpressionContext.class);
		}
		public OC_PowerOfExpressionContext oC_PowerOfExpression(int i) {
			return getRuleContext(OC_PowerOfExpressionContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_MultiplyDivideModuloExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_MultiplyDivideModuloExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_MultiplyDivideModuloExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_MultiplyDivideModuloExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_MultiplyDivideModuloExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_MultiplyDivideModuloExpressionContext oC_MultiplyDivideModuloExpression() throws RecognitionException {
		OC_MultiplyDivideModuloExpressionContext _localctx = new OC_MultiplyDivideModuloExpressionContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_oC_MultiplyDivideModuloExpression);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(937);
			oC_PowerOfExpression();
			setState(964);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,164,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					setState(962);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,163,_ctx) ) {
					case 1:
						{
						{
						setState(939);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(938);
							match(SP);
							}
						}

						setState(941);
						match(T__5);
						setState(943);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(942);
							match(SP);
							}
						}

						setState(945);
						oC_PowerOfExpression();
						}
						}
						break;
					case 2:
						{
						{
						setState(947);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(946);
							match(SP);
							}
						}

						setState(949);
						match(T__14);
						setState(951);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(950);
							match(SP);
							}
						}

						setState(953);
						oC_PowerOfExpression();
						}
						}
						break;
					case 3:
						{
						{
						setState(955);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(954);
							match(SP);
							}
						}

						setState(957);
						match(T__15);
						setState(959);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(958);
							match(SP);
							}
						}

						setState(961);
						oC_PowerOfExpression();
						}
						}
						break;
					}
					} 
				}
				setState(966);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,164,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_PowerOfExpressionContext extends ParserRuleContext {
		public List<OC_UnaryAddOrSubtractExpressionContext> oC_UnaryAddOrSubtractExpression() {
			return getRuleContexts(OC_UnaryAddOrSubtractExpressionContext.class);
		}
		public OC_UnaryAddOrSubtractExpressionContext oC_UnaryAddOrSubtractExpression(int i) {
			return getRuleContext(OC_UnaryAddOrSubtractExpressionContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_PowerOfExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_PowerOfExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_PowerOfExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_PowerOfExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_PowerOfExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_PowerOfExpressionContext oC_PowerOfExpression() throws RecognitionException {
		OC_PowerOfExpressionContext _localctx = new OC_PowerOfExpressionContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_oC_PowerOfExpression);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(967);
			oC_UnaryAddOrSubtractExpression();
			setState(978);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,167,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(969);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(968);
						match(SP);
						}
					}

					setState(971);
					match(T__16);
					setState(973);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(972);
						match(SP);
						}
					}

					setState(975);
					oC_UnaryAddOrSubtractExpression();
					}
					} 
				}
				setState(980);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,167,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_UnaryAddOrSubtractExpressionContext extends ParserRuleContext {
		public OC_StringListNullOperatorExpressionContext oC_StringListNullOperatorExpression() {
			return getRuleContext(OC_StringListNullOperatorExpressionContext.class,0);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_UnaryAddOrSubtractExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_UnaryAddOrSubtractExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_UnaryAddOrSubtractExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_UnaryAddOrSubtractExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_UnaryAddOrSubtractExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_UnaryAddOrSubtractExpressionContext oC_UnaryAddOrSubtractExpression() throws RecognitionException {
		OC_UnaryAddOrSubtractExpressionContext _localctx = new OC_UnaryAddOrSubtractExpressionContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_oC_UnaryAddOrSubtractExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(987);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__4 || _la==T__13) {
				{
				{
				setState(981);
				_la = _input.LA(1);
				if ( !(_la==T__4 || _la==T__13) ) {
				_errHandler.recoverInline(this);
				}
				else {
					if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
					_errHandler.reportMatch(this);
					consume();
				}
				setState(983);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(982);
					match(SP);
					}
				}

				}
				}
				setState(989);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(990);
			oC_StringListNullOperatorExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_StringListNullOperatorExpressionContext extends ParserRuleContext {
		public List<OC_PropertyOrLabelsExpressionContext> oC_PropertyOrLabelsExpression() {
			return getRuleContexts(OC_PropertyOrLabelsExpressionContext.class);
		}
		public OC_PropertyOrLabelsExpressionContext oC_PropertyOrLabelsExpression(int i) {
			return getRuleContext(OC_PropertyOrLabelsExpressionContext.class,i);
		}
		public List<OC_ExpressionContext> oC_Expression() {
			return getRuleContexts(OC_ExpressionContext.class);
		}
		public OC_ExpressionContext oC_Expression(int i) {
			return getRuleContext(OC_ExpressionContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public List<TerminalNode> IS() { return getTokens(CypherParser.IS); }
		public TerminalNode IS(int i) {
			return getToken(CypherParser.IS, i);
		}
		public List<TerminalNode> NULL() { return getTokens(CypherParser.NULL); }
		public TerminalNode NULL(int i) {
			return getToken(CypherParser.NULL, i);
		}
		public List<TerminalNode> NOT() { return getTokens(CypherParser.NOT); }
		public TerminalNode NOT(int i) {
			return getToken(CypherParser.NOT, i);
		}
		public List<TerminalNode> IN() { return getTokens(CypherParser.IN); }
		public TerminalNode IN(int i) {
			return getToken(CypherParser.IN, i);
		}
		public List<TerminalNode> STARTS() { return getTokens(CypherParser.STARTS); }
		public TerminalNode STARTS(int i) {
			return getToken(CypherParser.STARTS, i);
		}
		public List<TerminalNode> WITH() { return getTokens(CypherParser.WITH); }
		public TerminalNode WITH(int i) {
			return getToken(CypherParser.WITH, i);
		}
		public List<TerminalNode> ENDS() { return getTokens(CypherParser.ENDS); }
		public TerminalNode ENDS(int i) {
			return getToken(CypherParser.ENDS, i);
		}
		public List<TerminalNode> CONTAINS() { return getTokens(CypherParser.CONTAINS); }
		public TerminalNode CONTAINS(int i) {
			return getToken(CypherParser.CONTAINS, i);
		}
		public OC_StringListNullOperatorExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_StringListNullOperatorExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_StringListNullOperatorExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_StringListNullOperatorExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_StringListNullOperatorExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_StringListNullOperatorExpressionContext oC_StringListNullOperatorExpression() throws RecognitionException {
		OC_StringListNullOperatorExpressionContext _localctx = new OC_StringListNullOperatorExpressionContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_oC_StringListNullOperatorExpression);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(992);
			oC_PropertyOrLabelsExpression();
			setState(1042);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,177,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					setState(1040);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,176,_ctx) ) {
					case 1:
						{
						{
						setState(994);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(993);
							match(SP);
							}
						}

						setState(996);
						match(T__8);
						setState(997);
						oC_Expression();
						setState(998);
						match(T__9);
						}
						}
						break;
					case 2:
						{
						{
						setState(1001);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(1000);
							match(SP);
							}
						}

						setState(1003);
						match(T__8);
						setState(1005);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__4) | (1L << T__6) | (1L << T__8) | (1L << T__13) | (1L << T__23) | (1L << T__25) | (1L << ALL))) != 0) || ((((_la - 76)) & ~0x3f) == 0 && ((1L << (_la - 76)) & ((1L << (NOT - 76)) | (1L << (NULL - 76)) | (1L << (COUNT - 76)) | (1L << (FILTER - 76)) | (1L << (EXTRACT - 76)) | (1L << (ANY - 76)) | (1L << (NONE - 76)) | (1L << (SINGLE - 76)) | (1L << (TRUE - 76)) | (1L << (FALSE - 76)) | (1L << (EXISTS - 76)) | (1L << (CASE - 76)) | (1L << (StringLiteral - 76)) | (1L << (HexInteger - 76)) | (1L << (DecimalInteger - 76)) | (1L << (OctalInteger - 76)) | (1L << (HexLetter - 76)) | (1L << (ExponentDecimalReal - 76)) | (1L << (RegularDecimalReal - 76)) | (1L << (UnescapedSymbolicName - 76)) | (1L << (EscapedSymbolicName - 76)))) != 0)) {
							{
							setState(1004);
							oC_Expression();
							}
						}

						setState(1007);
						match(T__12);
						setState(1009);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__4) | (1L << T__6) | (1L << T__8) | (1L << T__13) | (1L << T__23) | (1L << T__25) | (1L << ALL))) != 0) || ((((_la - 76)) & ~0x3f) == 0 && ((1L << (_la - 76)) & ((1L << (NOT - 76)) | (1L << (NULL - 76)) | (1L << (COUNT - 76)) | (1L << (FILTER - 76)) | (1L << (EXTRACT - 76)) | (1L << (ANY - 76)) | (1L << (NONE - 76)) | (1L << (SINGLE - 76)) | (1L << (TRUE - 76)) | (1L << (FALSE - 76)) | (1L << (EXISTS - 76)) | (1L << (CASE - 76)) | (1L << (StringLiteral - 76)) | (1L << (HexInteger - 76)) | (1L << (DecimalInteger - 76)) | (1L << (OctalInteger - 76)) | (1L << (HexLetter - 76)) | (1L << (ExponentDecimalReal - 76)) | (1L << (RegularDecimalReal - 76)) | (1L << (UnescapedSymbolicName - 76)) | (1L << (EscapedSymbolicName - 76)))) != 0)) {
							{
							setState(1008);
							oC_Expression();
							}
						}

						setState(1011);
						match(T__9);
						}
						}
						break;
					case 3:
						{
						{
						setState(1024);
						_errHandler.sync(this);
						switch ( getInterpreter().adaptivePredict(_input,174,_ctx) ) {
						case 1:
							{
							{
							setState(1012);
							match(SP);
							setState(1013);
							match(IN);
							}
							}
							break;
						case 2:
							{
							{
							setState(1014);
							match(SP);
							setState(1015);
							match(STARTS);
							setState(1016);
							match(SP);
							setState(1017);
							match(WITH);
							}
							}
							break;
						case 3:
							{
							{
							setState(1018);
							match(SP);
							setState(1019);
							match(ENDS);
							setState(1020);
							match(SP);
							setState(1021);
							match(WITH);
							}
							}
							break;
						case 4:
							{
							{
							setState(1022);
							match(SP);
							setState(1023);
							match(CONTAINS);
							}
							}
							break;
						}
						setState(1027);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(1026);
							match(SP);
							}
						}

						setState(1029);
						oC_PropertyOrLabelsExpression();
						}
						}
						break;
					case 4:
						{
						{
						setState(1030);
						match(SP);
						setState(1031);
						match(IS);
						setState(1032);
						match(SP);
						setState(1033);
						match(NULL);
						}
						}
						break;
					case 5:
						{
						{
						setState(1034);
						match(SP);
						setState(1035);
						match(IS);
						setState(1036);
						match(SP);
						setState(1037);
						match(NOT);
						setState(1038);
						match(SP);
						setState(1039);
						match(NULL);
						}
						}
						break;
					}
					} 
				}
				setState(1044);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,177,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_PropertyOrLabelsExpressionContext extends ParserRuleContext {
		public OC_AtomContext oC_Atom() {
			return getRuleContext(OC_AtomContext.class,0);
		}
		public List<OC_PropertyLookupContext> oC_PropertyLookup() {
			return getRuleContexts(OC_PropertyLookupContext.class);
		}
		public OC_PropertyLookupContext oC_PropertyLookup(int i) {
			return getRuleContext(OC_PropertyLookupContext.class,i);
		}
		public OC_NodeLabelsContext oC_NodeLabels() {
			return getRuleContext(OC_NodeLabelsContext.class,0);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_PropertyOrLabelsExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_PropertyOrLabelsExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_PropertyOrLabelsExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_PropertyOrLabelsExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_PropertyOrLabelsExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_PropertyOrLabelsExpressionContext oC_PropertyOrLabelsExpression() throws RecognitionException {
		OC_PropertyOrLabelsExpressionContext _localctx = new OC_PropertyOrLabelsExpressionContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_oC_PropertyOrLabelsExpression);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1045);
			oC_Atom();
			setState(1052);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,179,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1047);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(1046);
						match(SP);
						}
					}

					setState(1049);
					oC_PropertyLookup();
					}
					} 
				}
				setState(1054);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,179,_ctx);
			}
			setState(1059);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,181,_ctx) ) {
			case 1:
				{
				setState(1056);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1055);
					match(SP);
					}
				}

				setState(1058);
				oC_NodeLabels();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_AtomContext extends ParserRuleContext {
		public OC_LiteralContext oC_Literal() {
			return getRuleContext(OC_LiteralContext.class,0);
		}
		public OC_ParameterContext oC_Parameter() {
			return getRuleContext(OC_ParameterContext.class,0);
		}
		public OC_CaseExpressionContext oC_CaseExpression() {
			return getRuleContext(OC_CaseExpressionContext.class,0);
		}
		public TerminalNode COUNT() { return getToken(CypherParser.COUNT, 0); }
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_ListComprehensionContext oC_ListComprehension() {
			return getRuleContext(OC_ListComprehensionContext.class,0);
		}
		public OC_PatternComprehensionContext oC_PatternComprehension() {
			return getRuleContext(OC_PatternComprehensionContext.class,0);
		}
		public TerminalNode FILTER() { return getToken(CypherParser.FILTER, 0); }
		public OC_FilterExpressionContext oC_FilterExpression() {
			return getRuleContext(OC_FilterExpressionContext.class,0);
		}
		public TerminalNode EXTRACT() { return getToken(CypherParser.EXTRACT, 0); }
		public OC_ExpressionContext oC_Expression() {
			return getRuleContext(OC_ExpressionContext.class,0);
		}
		public TerminalNode ALL() { return getToken(CypherParser.ALL, 0); }
		public TerminalNode ANY() { return getToken(CypherParser.ANY, 0); }
		public TerminalNode NONE() { return getToken(CypherParser.NONE, 0); }
		public TerminalNode SINGLE() { return getToken(CypherParser.SINGLE, 0); }
		public OC_RelationshipsPatternContext oC_RelationshipsPattern() {
			return getRuleContext(OC_RelationshipsPatternContext.class,0);
		}
		public OC_ParenthesizedExpressionContext oC_ParenthesizedExpression() {
			return getRuleContext(OC_ParenthesizedExpressionContext.class,0);
		}
		public OC_FunctionInvocationContext oC_FunctionInvocation() {
			return getRuleContext(OC_FunctionInvocationContext.class,0);
		}
		public OC_VariableContext oC_Variable() {
			return getRuleContext(OC_VariableContext.class,0);
		}
		public OC_AtomContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Atom; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Atom(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Atom(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Atom(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_AtomContext oC_Atom() throws RecognitionException {
		OC_AtomContext _localctx = new OC_AtomContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_oC_Atom);
		int _la;
		try {
			setState(1174);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,205,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1061);
				oC_Literal();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1062);
				oC_Parameter();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1063);
				oC_CaseExpression();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				{
				setState(1064);
				match(COUNT);
				setState(1066);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1065);
					match(SP);
					}
				}

				setState(1068);
				match(T__6);
				setState(1070);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1069);
					match(SP);
					}
				}

				setState(1072);
				match(T__5);
				setState(1074);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1073);
					match(SP);
					}
				}

				setState(1076);
				match(T__7);
				}
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(1077);
				oC_ListComprehension();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(1078);
				oC_PatternComprehension();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				{
				setState(1079);
				match(FILTER);
				setState(1081);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1080);
					match(SP);
					}
				}

				setState(1083);
				match(T__6);
				setState(1085);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1084);
					match(SP);
					}
				}

				setState(1087);
				oC_FilterExpression();
				setState(1089);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1088);
					match(SP);
					}
				}

				setState(1091);
				match(T__7);
				}
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				{
				setState(1093);
				match(EXTRACT);
				setState(1095);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1094);
					match(SP);
					}
				}

				setState(1097);
				match(T__6);
				setState(1099);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1098);
					match(SP);
					}
				}

				setState(1101);
				oC_FilterExpression();
				setState(1103);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,190,_ctx) ) {
				case 1:
					{
					setState(1102);
					match(SP);
					}
					break;
				}
				setState(1110);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==T__11 || _la==SP) {
					{
					setState(1106);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(1105);
						match(SP);
						}
					}

					setState(1108);
					match(T__11);
					setState(1109);
					oC_Expression();
					}
				}

				setState(1112);
				match(T__7);
				}
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				{
				setState(1114);
				match(ALL);
				setState(1116);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1115);
					match(SP);
					}
				}

				setState(1118);
				match(T__6);
				setState(1120);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1119);
					match(SP);
					}
				}

				setState(1122);
				oC_FilterExpression();
				setState(1124);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1123);
					match(SP);
					}
				}

				setState(1126);
				match(T__7);
				}
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				{
				setState(1128);
				match(ANY);
				setState(1130);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1129);
					match(SP);
					}
				}

				setState(1132);
				match(T__6);
				setState(1134);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1133);
					match(SP);
					}
				}

				setState(1136);
				oC_FilterExpression();
				setState(1138);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1137);
					match(SP);
					}
				}

				setState(1140);
				match(T__7);
				}
				}
				break;
			case 11:
				enterOuterAlt(_localctx, 11);
				{
				{
				setState(1142);
				match(NONE);
				setState(1144);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1143);
					match(SP);
					}
				}

				setState(1146);
				match(T__6);
				setState(1148);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1147);
					match(SP);
					}
				}

				setState(1150);
				oC_FilterExpression();
				setState(1152);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1151);
					match(SP);
					}
				}

				setState(1154);
				match(T__7);
				}
				}
				break;
			case 12:
				enterOuterAlt(_localctx, 12);
				{
				{
				setState(1156);
				match(SINGLE);
				setState(1158);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1157);
					match(SP);
					}
				}

				setState(1160);
				match(T__6);
				setState(1162);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1161);
					match(SP);
					}
				}

				setState(1164);
				oC_FilterExpression();
				setState(1166);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1165);
					match(SP);
					}
				}

				setState(1168);
				match(T__7);
				}
				}
				break;
			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(1170);
				oC_RelationshipsPattern();
				}
				break;
			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(1171);
				oC_ParenthesizedExpression();
				}
				break;
			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(1172);
				oC_FunctionInvocation();
				}
				break;
			case 16:
				enterOuterAlt(_localctx, 16);
				{
				setState(1173);
				oC_Variable();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_LiteralContext extends ParserRuleContext {
		public OC_NumberLiteralContext oC_NumberLiteral() {
			return getRuleContext(OC_NumberLiteralContext.class,0);
		}
		public TerminalNode StringLiteral() { return getToken(CypherParser.StringLiteral, 0); }
		public OC_BooleanLiteralContext oC_BooleanLiteral() {
			return getRuleContext(OC_BooleanLiteralContext.class,0);
		}
		public TerminalNode NULL() { return getToken(CypherParser.NULL, 0); }
		public OC_MapLiteralContext oC_MapLiteral() {
			return getRuleContext(OC_MapLiteralContext.class,0);
		}
		public OC_ListLiteralContext oC_ListLiteral() {
			return getRuleContext(OC_ListLiteralContext.class,0);
		}
		public OC_LiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Literal; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Literal(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Literal(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Literal(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_LiteralContext oC_Literal() throws RecognitionException {
		OC_LiteralContext _localctx = new OC_LiteralContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_oC_Literal);
		try {
			setState(1182);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case HexInteger:
			case DecimalInteger:
			case OctalInteger:
			case ExponentDecimalReal:
			case RegularDecimalReal:
				enterOuterAlt(_localctx, 1);
				{
				setState(1176);
				oC_NumberLiteral();
				}
				break;
			case StringLiteral:
				enterOuterAlt(_localctx, 2);
				{
				setState(1177);
				match(StringLiteral);
				}
				break;
			case TRUE:
			case FALSE:
				enterOuterAlt(_localctx, 3);
				{
				setState(1178);
				oC_BooleanLiteral();
				}
				break;
			case NULL:
				enterOuterAlt(_localctx, 4);
				{
				setState(1179);
				match(NULL);
				}
				break;
			case T__23:
				enterOuterAlt(_localctx, 5);
				{
				setState(1180);
				oC_MapLiteral();
				}
				break;
			case T__8:
				enterOuterAlt(_localctx, 6);
				{
				setState(1181);
				oC_ListLiteral();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_BooleanLiteralContext extends ParserRuleContext {
		public TerminalNode TRUE() { return getToken(CypherParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(CypherParser.FALSE, 0); }
		public OC_BooleanLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_BooleanLiteral; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_BooleanLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_BooleanLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_BooleanLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_BooleanLiteralContext oC_BooleanLiteral() throws RecognitionException {
		OC_BooleanLiteralContext _localctx = new OC_BooleanLiteralContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_oC_BooleanLiteral);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1184);
			_la = _input.LA(1);
			if ( !(_la==TRUE || _la==FALSE) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_ListLiteralContext extends ParserRuleContext {
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public List<OC_ExpressionContext> oC_Expression() {
			return getRuleContexts(OC_ExpressionContext.class);
		}
		public OC_ExpressionContext oC_Expression(int i) {
			return getRuleContext(OC_ExpressionContext.class,i);
		}
		public OC_ListLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_ListLiteral; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_ListLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_ListLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_ListLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_ListLiteralContext oC_ListLiteral() throws RecognitionException {
		OC_ListLiteralContext _localctx = new OC_ListLiteralContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_oC_ListLiteral);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1186);
			match(T__8);
			setState(1188);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1187);
				match(SP);
				}
			}

			setState(1207);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__4) | (1L << T__6) | (1L << T__8) | (1L << T__13) | (1L << T__23) | (1L << T__25) | (1L << ALL))) != 0) || ((((_la - 76)) & ~0x3f) == 0 && ((1L << (_la - 76)) & ((1L << (NOT - 76)) | (1L << (NULL - 76)) | (1L << (COUNT - 76)) | (1L << (FILTER - 76)) | (1L << (EXTRACT - 76)) | (1L << (ANY - 76)) | (1L << (NONE - 76)) | (1L << (SINGLE - 76)) | (1L << (TRUE - 76)) | (1L << (FALSE - 76)) | (1L << (EXISTS - 76)) | (1L << (CASE - 76)) | (1L << (StringLiteral - 76)) | (1L << (HexInteger - 76)) | (1L << (DecimalInteger - 76)) | (1L << (OctalInteger - 76)) | (1L << (HexLetter - 76)) | (1L << (ExponentDecimalReal - 76)) | (1L << (RegularDecimalReal - 76)) | (1L << (UnescapedSymbolicName - 76)) | (1L << (EscapedSymbolicName - 76)))) != 0)) {
				{
				setState(1190);
				oC_Expression();
				setState(1192);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1191);
					match(SP);
					}
				}

				setState(1204);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(1194);
					match(T__1);
					setState(1196);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(1195);
						match(SP);
						}
					}

					setState(1198);
					oC_Expression();
					setState(1200);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(1199);
						match(SP);
						}
					}

					}
					}
					setState(1206);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1209);
			match(T__9);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_PartialComparisonExpressionContext extends ParserRuleContext {
		public OC_AddOrSubtractExpressionContext oC_AddOrSubtractExpression() {
			return getRuleContext(OC_AddOrSubtractExpressionContext.class,0);
		}
		public TerminalNode SP() { return getToken(CypherParser.SP, 0); }
		public OC_PartialComparisonExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_PartialComparisonExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_PartialComparisonExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_PartialComparisonExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_PartialComparisonExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_PartialComparisonExpressionContext oC_PartialComparisonExpression() throws RecognitionException {
		OC_PartialComparisonExpressionContext _localctx = new OC_PartialComparisonExpressionContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_oC_PartialComparisonExpression);
		int _la;
		try {
			setState(1241);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__2:
				enterOuterAlt(_localctx, 1);
				{
				{
				setState(1211);
				match(T__2);
				setState(1213);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1212);
					match(SP);
					}
				}

				setState(1215);
				oC_AddOrSubtractExpression();
				}
				}
				break;
			case T__17:
				enterOuterAlt(_localctx, 2);
				{
				{
				setState(1216);
				match(T__17);
				setState(1218);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1217);
					match(SP);
					}
				}

				setState(1220);
				oC_AddOrSubtractExpression();
				}
				}
				break;
			case T__18:
				enterOuterAlt(_localctx, 3);
				{
				{
				setState(1221);
				match(T__18);
				setState(1223);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1222);
					match(SP);
					}
				}

				setState(1225);
				oC_AddOrSubtractExpression();
				}
				}
				break;
			case T__19:
				enterOuterAlt(_localctx, 4);
				{
				{
				setState(1226);
				match(T__19);
				setState(1228);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1227);
					match(SP);
					}
				}

				setState(1230);
				oC_AddOrSubtractExpression();
				}
				}
				break;
			case T__20:
				enterOuterAlt(_localctx, 5);
				{
				{
				setState(1231);
				match(T__20);
				setState(1233);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1232);
					match(SP);
					}
				}

				setState(1235);
				oC_AddOrSubtractExpression();
				}
				}
				break;
			case T__21:
				enterOuterAlt(_localctx, 6);
				{
				{
				setState(1236);
				match(T__21);
				setState(1238);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1237);
					match(SP);
					}
				}

				setState(1240);
				oC_AddOrSubtractExpression();
				}
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_ParenthesizedExpressionContext extends ParserRuleContext {
		public OC_ExpressionContext oC_Expression() {
			return getRuleContext(OC_ExpressionContext.class,0);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_ParenthesizedExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_ParenthesizedExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_ParenthesizedExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_ParenthesizedExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_ParenthesizedExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_ParenthesizedExpressionContext oC_ParenthesizedExpression() throws RecognitionException {
		OC_ParenthesizedExpressionContext _localctx = new OC_ParenthesizedExpressionContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_oC_ParenthesizedExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1243);
			match(T__6);
			setState(1245);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1244);
				match(SP);
				}
			}

			setState(1247);
			oC_Expression();
			setState(1249);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1248);
				match(SP);
				}
			}

			setState(1251);
			match(T__7);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_RelationshipsPatternContext extends ParserRuleContext {
		public OC_NodePatternContext oC_NodePattern() {
			return getRuleContext(OC_NodePatternContext.class,0);
		}
		public List<OC_PatternElementChainContext> oC_PatternElementChain() {
			return getRuleContexts(OC_PatternElementChainContext.class);
		}
		public OC_PatternElementChainContext oC_PatternElementChain(int i) {
			return getRuleContext(OC_PatternElementChainContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_RelationshipsPatternContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_RelationshipsPattern; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_RelationshipsPattern(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_RelationshipsPattern(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_RelationshipsPattern(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_RelationshipsPatternContext oC_RelationshipsPattern() throws RecognitionException {
		OC_RelationshipsPatternContext _localctx = new OC_RelationshipsPatternContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_oC_RelationshipsPattern);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1253);
			oC_NodePattern();
			setState(1258); 
			_errHandler.sync(this);
			_alt = 1;
			do {
				switch (_alt) {
				case 1:
					{
					{
					setState(1255);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(1254);
						match(SP);
						}
					}

					setState(1257);
					oC_PatternElementChain();
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1260); 
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,223,_ctx);
			} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_FilterExpressionContext extends ParserRuleContext {
		public OC_IdInCollContext oC_IdInColl() {
			return getRuleContext(OC_IdInCollContext.class,0);
		}
		public OC_WhereContext oC_Where() {
			return getRuleContext(OC_WhereContext.class,0);
		}
		public TerminalNode SP() { return getToken(CypherParser.SP, 0); }
		public OC_FilterExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_FilterExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_FilterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_FilterExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_FilterExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_FilterExpressionContext oC_FilterExpression() throws RecognitionException {
		OC_FilterExpressionContext _localctx = new OC_FilterExpressionContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_oC_FilterExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1262);
			oC_IdInColl();
			setState(1267);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,225,_ctx) ) {
			case 1:
				{
				setState(1264);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1263);
					match(SP);
					}
				}

				setState(1266);
				oC_Where();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_IdInCollContext extends ParserRuleContext {
		public OC_VariableContext oC_Variable() {
			return getRuleContext(OC_VariableContext.class,0);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public TerminalNode IN() { return getToken(CypherParser.IN, 0); }
		public OC_ExpressionContext oC_Expression() {
			return getRuleContext(OC_ExpressionContext.class,0);
		}
		public OC_IdInCollContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_IdInColl; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_IdInColl(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_IdInColl(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_IdInColl(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_IdInCollContext oC_IdInColl() throws RecognitionException {
		OC_IdInCollContext _localctx = new OC_IdInCollContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_oC_IdInColl);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1269);
			oC_Variable();
			setState(1270);
			match(SP);
			setState(1271);
			match(IN);
			setState(1272);
			match(SP);
			setState(1273);
			oC_Expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_FunctionInvocationContext extends ParserRuleContext {
		public OC_FunctionNameContext oC_FunctionName() {
			return getRuleContext(OC_FunctionNameContext.class,0);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public TerminalNode DISTINCT() { return getToken(CypherParser.DISTINCT, 0); }
		public List<OC_ExpressionContext> oC_Expression() {
			return getRuleContexts(OC_ExpressionContext.class);
		}
		public OC_ExpressionContext oC_Expression(int i) {
			return getRuleContext(OC_ExpressionContext.class,i);
		}
		public OC_FunctionInvocationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_FunctionInvocation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_FunctionInvocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_FunctionInvocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_FunctionInvocation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_FunctionInvocationContext oC_FunctionInvocation() throws RecognitionException {
		OC_FunctionInvocationContext _localctx = new OC_FunctionInvocationContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_oC_FunctionInvocation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1275);
			oC_FunctionName();
			setState(1277);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1276);
				match(SP);
				}
			}

			setState(1279);
			match(T__6);
			setState(1281);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1280);
				match(SP);
				}
			}

			setState(1287);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==DISTINCT) {
				{
				setState(1283);
				match(DISTINCT);
				setState(1285);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1284);
					match(SP);
					}
				}

				}
			}

			setState(1306);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__4) | (1L << T__6) | (1L << T__8) | (1L << T__13) | (1L << T__23) | (1L << T__25) | (1L << ALL))) != 0) || ((((_la - 76)) & ~0x3f) == 0 && ((1L << (_la - 76)) & ((1L << (NOT - 76)) | (1L << (NULL - 76)) | (1L << (COUNT - 76)) | (1L << (FILTER - 76)) | (1L << (EXTRACT - 76)) | (1L << (ANY - 76)) | (1L << (NONE - 76)) | (1L << (SINGLE - 76)) | (1L << (TRUE - 76)) | (1L << (FALSE - 76)) | (1L << (EXISTS - 76)) | (1L << (CASE - 76)) | (1L << (StringLiteral - 76)) | (1L << (HexInteger - 76)) | (1L << (DecimalInteger - 76)) | (1L << (OctalInteger - 76)) | (1L << (HexLetter - 76)) | (1L << (ExponentDecimalReal - 76)) | (1L << (RegularDecimalReal - 76)) | (1L << (UnescapedSymbolicName - 76)) | (1L << (EscapedSymbolicName - 76)))) != 0)) {
				{
				setState(1289);
				oC_Expression();
				setState(1291);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1290);
					match(SP);
					}
				}

				setState(1303);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(1293);
					match(T__1);
					setState(1295);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(1294);
						match(SP);
						}
					}

					setState(1297);
					oC_Expression();
					setState(1299);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(1298);
						match(SP);
						}
					}

					}
					}
					setState(1305);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1308);
			match(T__7);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_FunctionNameContext extends ParserRuleContext {
		public OC_SymbolicNameContext oC_SymbolicName() {
			return getRuleContext(OC_SymbolicNameContext.class,0);
		}
		public TerminalNode EXISTS() { return getToken(CypherParser.EXISTS, 0); }
		public OC_FunctionNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_FunctionName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_FunctionName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_FunctionName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_FunctionName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_FunctionNameContext oC_FunctionName() throws RecognitionException {
		OC_FunctionNameContext _localctx = new OC_FunctionNameContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_oC_FunctionName);
		try {
			setState(1312);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case COUNT:
			case FILTER:
			case EXTRACT:
			case ANY:
			case NONE:
			case SINGLE:
			case HexLetter:
			case UnescapedSymbolicName:
			case EscapedSymbolicName:
				enterOuterAlt(_localctx, 1);
				{
				setState(1310);
				oC_SymbolicName();
				}
				break;
			case EXISTS:
				enterOuterAlt(_localctx, 2);
				{
				setState(1311);
				match(EXISTS);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_ExplicitProcedureInvocationContext extends ParserRuleContext {
		public OC_ProcedureNameContext oC_ProcedureName() {
			return getRuleContext(OC_ProcedureNameContext.class,0);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public List<OC_ExpressionContext> oC_Expression() {
			return getRuleContexts(OC_ExpressionContext.class);
		}
		public OC_ExpressionContext oC_Expression(int i) {
			return getRuleContext(OC_ExpressionContext.class,i);
		}
		public OC_ExplicitProcedureInvocationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_ExplicitProcedureInvocation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_ExplicitProcedureInvocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_ExplicitProcedureInvocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_ExplicitProcedureInvocation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_ExplicitProcedureInvocationContext oC_ExplicitProcedureInvocation() throws RecognitionException {
		OC_ExplicitProcedureInvocationContext _localctx = new OC_ExplicitProcedureInvocationContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_oC_ExplicitProcedureInvocation);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1314);
			oC_ProcedureName();
			setState(1316);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1315);
				match(SP);
				}
			}

			setState(1318);
			match(T__6);
			setState(1320);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1319);
				match(SP);
				}
			}

			setState(1339);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__4) | (1L << T__6) | (1L << T__8) | (1L << T__13) | (1L << T__23) | (1L << T__25) | (1L << ALL))) != 0) || ((((_la - 76)) & ~0x3f) == 0 && ((1L << (_la - 76)) & ((1L << (NOT - 76)) | (1L << (NULL - 76)) | (1L << (COUNT - 76)) | (1L << (FILTER - 76)) | (1L << (EXTRACT - 76)) | (1L << (ANY - 76)) | (1L << (NONE - 76)) | (1L << (SINGLE - 76)) | (1L << (TRUE - 76)) | (1L << (FALSE - 76)) | (1L << (EXISTS - 76)) | (1L << (CASE - 76)) | (1L << (StringLiteral - 76)) | (1L << (HexInteger - 76)) | (1L << (DecimalInteger - 76)) | (1L << (OctalInteger - 76)) | (1L << (HexLetter - 76)) | (1L << (ExponentDecimalReal - 76)) | (1L << (RegularDecimalReal - 76)) | (1L << (UnescapedSymbolicName - 76)) | (1L << (EscapedSymbolicName - 76)))) != 0)) {
				{
				setState(1322);
				oC_Expression();
				setState(1324);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1323);
					match(SP);
					}
				}

				setState(1336);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(1326);
					match(T__1);
					setState(1328);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(1327);
						match(SP);
						}
					}

					setState(1330);
					oC_Expression();
					setState(1332);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(1331);
						match(SP);
						}
					}

					}
					}
					setState(1338);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1341);
			match(T__7);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_ImplicitProcedureInvocationContext extends ParserRuleContext {
		public OC_ProcedureNameContext oC_ProcedureName() {
			return getRuleContext(OC_ProcedureNameContext.class,0);
		}
		public OC_ImplicitProcedureInvocationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_ImplicitProcedureInvocation; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_ImplicitProcedureInvocation(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_ImplicitProcedureInvocation(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_ImplicitProcedureInvocation(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_ImplicitProcedureInvocationContext oC_ImplicitProcedureInvocation() throws RecognitionException {
		OC_ImplicitProcedureInvocationContext _localctx = new OC_ImplicitProcedureInvocationContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_oC_ImplicitProcedureInvocation);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1343);
			oC_ProcedureName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_ProcedureResultFieldContext extends ParserRuleContext {
		public OC_SymbolicNameContext oC_SymbolicName() {
			return getRuleContext(OC_SymbolicNameContext.class,0);
		}
		public OC_ProcedureResultFieldContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_ProcedureResultField; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_ProcedureResultField(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_ProcedureResultField(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_ProcedureResultField(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_ProcedureResultFieldContext oC_ProcedureResultField() throws RecognitionException {
		OC_ProcedureResultFieldContext _localctx = new OC_ProcedureResultFieldContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_oC_ProcedureResultField);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1345);
			oC_SymbolicName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_ProcedureNameContext extends ParserRuleContext {
		public OC_NamespaceContext oC_Namespace() {
			return getRuleContext(OC_NamespaceContext.class,0);
		}
		public OC_SymbolicNameContext oC_SymbolicName() {
			return getRuleContext(OC_SymbolicNameContext.class,0);
		}
		public OC_ProcedureNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_ProcedureName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_ProcedureName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_ProcedureName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_ProcedureName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_ProcedureNameContext oC_ProcedureName() throws RecognitionException {
		OC_ProcedureNameContext _localctx = new OC_ProcedureNameContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_oC_ProcedureName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1347);
			oC_Namespace();
			setState(1348);
			oC_SymbolicName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_NamespaceContext extends ParserRuleContext {
		public List<OC_SymbolicNameContext> oC_SymbolicName() {
			return getRuleContexts(OC_SymbolicNameContext.class);
		}
		public OC_SymbolicNameContext oC_SymbolicName(int i) {
			return getRuleContext(OC_SymbolicNameContext.class,i);
		}
		public OC_NamespaceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Namespace; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Namespace(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Namespace(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Namespace(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_NamespaceContext oC_Namespace() throws RecognitionException {
		OC_NamespaceContext _localctx = new OC_NamespaceContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_oC_Namespace);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1355);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,243,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					{
					{
					setState(1350);
					oC_SymbolicName();
					setState(1351);
					match(T__22);
					}
					} 
				}
				setState(1357);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,243,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_ListComprehensionContext extends ParserRuleContext {
		public OC_FilterExpressionContext oC_FilterExpression() {
			return getRuleContext(OC_FilterExpressionContext.class,0);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_ExpressionContext oC_Expression() {
			return getRuleContext(OC_ExpressionContext.class,0);
		}
		public OC_ListComprehensionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_ListComprehension; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_ListComprehension(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_ListComprehension(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_ListComprehension(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_ListComprehensionContext oC_ListComprehension() throws RecognitionException {
		OC_ListComprehensionContext _localctx = new OC_ListComprehensionContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_oC_ListComprehension);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1358);
			match(T__8);
			setState(1360);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1359);
				match(SP);
				}
			}

			setState(1362);
			oC_FilterExpression();
			setState(1371);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,247,_ctx) ) {
			case 1:
				{
				setState(1364);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1363);
					match(SP);
					}
				}

				setState(1366);
				match(T__11);
				setState(1368);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1367);
					match(SP);
					}
				}

				setState(1370);
				oC_Expression();
				}
				break;
			}
			setState(1374);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1373);
				match(SP);
				}
			}

			setState(1376);
			match(T__9);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_PatternComprehensionContext extends ParserRuleContext {
		public OC_RelationshipsPatternContext oC_RelationshipsPattern() {
			return getRuleContext(OC_RelationshipsPatternContext.class,0);
		}
		public List<OC_ExpressionContext> oC_Expression() {
			return getRuleContexts(OC_ExpressionContext.class);
		}
		public OC_ExpressionContext oC_Expression(int i) {
			return getRuleContext(OC_ExpressionContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_VariableContext oC_Variable() {
			return getRuleContext(OC_VariableContext.class,0);
		}
		public TerminalNode WHERE() { return getToken(CypherParser.WHERE, 0); }
		public OC_PatternComprehensionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_PatternComprehension; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_PatternComprehension(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_PatternComprehension(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_PatternComprehension(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_PatternComprehensionContext oC_PatternComprehension() throws RecognitionException {
		OC_PatternComprehensionContext _localctx = new OC_PatternComprehensionContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_oC_PatternComprehension);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1378);
			match(T__8);
			setState(1380);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1379);
				match(SP);
				}
			}

			setState(1390);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (((((_la - 83)) & ~0x3f) == 0 && ((1L << (_la - 83)) & ((1L << (COUNT - 83)) | (1L << (FILTER - 83)) | (1L << (EXTRACT - 83)) | (1L << (ANY - 83)) | (1L << (NONE - 83)) | (1L << (SINGLE - 83)) | (1L << (HexLetter - 83)) | (1L << (UnescapedSymbolicName - 83)) | (1L << (EscapedSymbolicName - 83)))) != 0)) {
				{
				setState(1382);
				oC_Variable();
				setState(1384);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1383);
					match(SP);
					}
				}

				setState(1386);
				match(T__2);
				setState(1388);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1387);
					match(SP);
					}
				}

				}
			}

			setState(1392);
			oC_RelationshipsPattern();
			setState(1394);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1393);
				match(SP);
				}
			}

			setState(1404);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==WHERE) {
				{
				setState(1396);
				match(WHERE);
				setState(1398);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1397);
					match(SP);
					}
				}

				setState(1400);
				oC_Expression();
				setState(1402);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1401);
					match(SP);
					}
				}

				}
			}

			setState(1406);
			match(T__11);
			setState(1408);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1407);
				match(SP);
				}
			}

			setState(1410);
			oC_Expression();
			setState(1412);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1411);
				match(SP);
				}
			}

			setState(1414);
			match(T__9);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_PropertyLookupContext extends ParserRuleContext {
		public OC_PropertyKeyNameContext oC_PropertyKeyName() {
			return getRuleContext(OC_PropertyKeyNameContext.class,0);
		}
		public TerminalNode SP() { return getToken(CypherParser.SP, 0); }
		public OC_PropertyLookupContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_PropertyLookup; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_PropertyLookup(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_PropertyLookup(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_PropertyLookup(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_PropertyLookupContext oC_PropertyLookup() throws RecognitionException {
		OC_PropertyLookupContext _localctx = new OC_PropertyLookupContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_oC_PropertyLookup);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1416);
			match(T__22);
			setState(1418);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1417);
				match(SP);
				}
			}

			{
			setState(1420);
			oC_PropertyKeyName();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_CaseExpressionContext extends ParserRuleContext {
		public TerminalNode END() { return getToken(CypherParser.END, 0); }
		public TerminalNode ELSE() { return getToken(CypherParser.ELSE, 0); }
		public List<OC_ExpressionContext> oC_Expression() {
			return getRuleContexts(OC_ExpressionContext.class);
		}
		public OC_ExpressionContext oC_Expression(int i) {
			return getRuleContext(OC_ExpressionContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public TerminalNode CASE() { return getToken(CypherParser.CASE, 0); }
		public List<OC_CaseAlternativesContext> oC_CaseAlternatives() {
			return getRuleContexts(OC_CaseAlternativesContext.class);
		}
		public OC_CaseAlternativesContext oC_CaseAlternatives(int i) {
			return getRuleContext(OC_CaseAlternativesContext.class,i);
		}
		public OC_CaseExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_CaseExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_CaseExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_CaseExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_CaseExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_CaseExpressionContext oC_CaseExpression() throws RecognitionException {
		OC_CaseExpressionContext _localctx = new OC_CaseExpressionContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_oC_CaseExpression);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1444);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,265,_ctx) ) {
			case 1:
				{
				{
				setState(1422);
				match(CASE);
				setState(1427); 
				_errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						setState(1424);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(1423);
							match(SP);
							}
						}

						setState(1426);
						oC_CaseAlternatives();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					setState(1429); 
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,261,_ctx);
				} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
				}
				}
				break;
			case 2:
				{
				{
				setState(1431);
				match(CASE);
				setState(1433);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1432);
					match(SP);
					}
				}

				setState(1435);
				oC_Expression();
				setState(1440); 
				_errHandler.sync(this);
				_alt = 1;
				do {
					switch (_alt) {
					case 1:
						{
						{
						setState(1437);
						_errHandler.sync(this);
						_la = _input.LA(1);
						if (_la==SP) {
							{
							setState(1436);
							match(SP);
							}
						}

						setState(1439);
						oC_CaseAlternatives();
						}
						}
						break;
					default:
						throw new NoViableAltException(this);
					}
					setState(1442); 
					_errHandler.sync(this);
					_alt = getInterpreter().adaptivePredict(_input,264,_ctx);
				} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
				}
				}
				break;
			}
			setState(1454);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,268,_ctx) ) {
			case 1:
				{
				setState(1447);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1446);
					match(SP);
					}
				}

				setState(1449);
				match(ELSE);
				setState(1451);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1450);
					match(SP);
					}
				}

				setState(1453);
				oC_Expression();
				}
				break;
			}
			setState(1457);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1456);
				match(SP);
				}
			}

			setState(1459);
			match(END);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_CaseAlternativesContext extends ParserRuleContext {
		public TerminalNode WHEN() { return getToken(CypherParser.WHEN, 0); }
		public List<OC_ExpressionContext> oC_Expression() {
			return getRuleContexts(OC_ExpressionContext.class);
		}
		public OC_ExpressionContext oC_Expression(int i) {
			return getRuleContext(OC_ExpressionContext.class,i);
		}
		public TerminalNode THEN() { return getToken(CypherParser.THEN, 0); }
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_CaseAlternativesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_CaseAlternatives; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_CaseAlternatives(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_CaseAlternatives(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_CaseAlternatives(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_CaseAlternativesContext oC_CaseAlternatives() throws RecognitionException {
		OC_CaseAlternativesContext _localctx = new OC_CaseAlternativesContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_oC_CaseAlternatives);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1461);
			match(WHEN);
			setState(1463);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1462);
				match(SP);
				}
			}

			setState(1465);
			oC_Expression();
			setState(1467);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1466);
				match(SP);
				}
			}

			setState(1469);
			match(THEN);
			setState(1471);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1470);
				match(SP);
				}
			}

			setState(1473);
			oC_Expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_VariableContext extends ParserRuleContext {
		public OC_SymbolicNameContext oC_SymbolicName() {
			return getRuleContext(OC_SymbolicNameContext.class,0);
		}
		public OC_VariableContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Variable; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Variable(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Variable(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Variable(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_VariableContext oC_Variable() throws RecognitionException {
		OC_VariableContext _localctx = new OC_VariableContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_oC_Variable);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1475);
			oC_SymbolicName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_NumberLiteralContext extends ParserRuleContext {
		public OC_DoubleLiteralContext oC_DoubleLiteral() {
			return getRuleContext(OC_DoubleLiteralContext.class,0);
		}
		public OC_IntegerLiteralContext oC_IntegerLiteral() {
			return getRuleContext(OC_IntegerLiteralContext.class,0);
		}
		public OC_NumberLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_NumberLiteral; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_NumberLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_NumberLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_NumberLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_NumberLiteralContext oC_NumberLiteral() throws RecognitionException {
		OC_NumberLiteralContext _localctx = new OC_NumberLiteralContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_oC_NumberLiteral);
		try {
			setState(1479);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ExponentDecimalReal:
			case RegularDecimalReal:
				enterOuterAlt(_localctx, 1);
				{
				setState(1477);
				oC_DoubleLiteral();
				}
				break;
			case HexInteger:
			case DecimalInteger:
			case OctalInteger:
				enterOuterAlt(_localctx, 2);
				{
				setState(1478);
				oC_IntegerLiteral();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_MapLiteralContext extends ParserRuleContext {
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public List<OC_PropertyKeyNameContext> oC_PropertyKeyName() {
			return getRuleContexts(OC_PropertyKeyNameContext.class);
		}
		public OC_PropertyKeyNameContext oC_PropertyKeyName(int i) {
			return getRuleContext(OC_PropertyKeyNameContext.class,i);
		}
		public List<OC_ExpressionContext> oC_Expression() {
			return getRuleContexts(OC_ExpressionContext.class);
		}
		public OC_ExpressionContext oC_Expression(int i) {
			return getRuleContext(OC_ExpressionContext.class,i);
		}
		public OC_MapLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_MapLiteral; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_MapLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_MapLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_MapLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_MapLiteralContext oC_MapLiteral() throws RecognitionException {
		OC_MapLiteralContext _localctx = new OC_MapLiteralContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_oC_MapLiteral);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1481);
			match(T__23);
			setState(1483);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if (_la==SP) {
				{
				setState(1482);
				match(SP);
				}
			}

			setState(1518);
			_errHandler.sync(this);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << UNION) | (1L << ALL) | (1L << OPTIONAL) | (1L << MATCH) | (1L << UNWIND) | (1L << AS) | (1L << MERGE) | (1L << ON) | (1L << CREATE) | (1L << SET) | (1L << DETACH) | (1L << DELETE) | (1L << REMOVE) | (1L << WITH) | (1L << DISTINCT) | (1L << RETURN))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (ORDER - 64)) | (1L << (BY - 64)) | (1L << (L_SKIP - 64)) | (1L << (LIMIT - 64)) | (1L << (ASCENDING - 64)) | (1L << (ASC - 64)) | (1L << (DESCENDING - 64)) | (1L << (DESC - 64)) | (1L << (WHERE - 64)) | (1L << (OR - 64)) | (1L << (XOR - 64)) | (1L << (AND - 64)) | (1L << (NOT - 64)) | (1L << (IN - 64)) | (1L << (STARTS - 64)) | (1L << (ENDS - 64)) | (1L << (CONTAINS - 64)) | (1L << (IS - 64)) | (1L << (NULL - 64)) | (1L << (COUNT - 64)) | (1L << (FILTER - 64)) | (1L << (EXTRACT - 64)) | (1L << (ANY - 64)) | (1L << (NONE - 64)) | (1L << (SINGLE - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (EXISTS - 64)) | (1L << (CASE - 64)) | (1L << (ELSE - 64)) | (1L << (END - 64)) | (1L << (WHEN - 64)) | (1L << (THEN - 64)) | (1L << (HexLetter - 64)) | (1L << (CONSTRAINT - 64)) | (1L << (DO - 64)) | (1L << (FOR - 64)) | (1L << (REQUIRE - 64)) | (1L << (UNIQUE - 64)) | (1L << (MANDATORY - 64)) | (1L << (SCALAR - 64)) | (1L << (OF - 64)) | (1L << (ADD - 64)) | (1L << (DROP - 64)) | (1L << (UnescapedSymbolicName - 64)) | (1L << (EscapedSymbolicName - 64)))) != 0)) {
				{
				setState(1485);
				oC_PropertyKeyName();
				setState(1487);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1486);
					match(SP);
					}
				}

				setState(1489);
				match(T__10);
				setState(1491);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1490);
					match(SP);
					}
				}

				setState(1493);
				oC_Expression();
				setState(1495);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==SP) {
					{
					setState(1494);
					match(SP);
					}
				}

				setState(1515);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==T__1) {
					{
					{
					setState(1497);
					match(T__1);
					setState(1499);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(1498);
						match(SP);
						}
					}

					setState(1501);
					oC_PropertyKeyName();
					setState(1503);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(1502);
						match(SP);
						}
					}

					setState(1505);
					match(T__10);
					setState(1507);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(1506);
						match(SP);
						}
					}

					setState(1509);
					oC_Expression();
					setState(1511);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(1510);
						match(SP);
						}
					}

					}
					}
					setState(1517);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(1520);
			match(T__24);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_ParameterContext extends ParserRuleContext {
		public OC_SymbolicNameContext oC_SymbolicName() {
			return getRuleContext(OC_SymbolicNameContext.class,0);
		}
		public TerminalNode DecimalInteger() { return getToken(CypherParser.DecimalInteger, 0); }
		public OC_ParameterContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Parameter; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Parameter(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Parameter(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Parameter(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_ParameterContext oC_Parameter() throws RecognitionException {
		OC_ParameterContext _localctx = new OC_ParameterContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_oC_Parameter);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1522);
			match(T__25);
			setState(1525);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case COUNT:
			case FILTER:
			case EXTRACT:
			case ANY:
			case NONE:
			case SINGLE:
			case HexLetter:
			case UnescapedSymbolicName:
			case EscapedSymbolicName:
				{
				setState(1523);
				oC_SymbolicName();
				}
				break;
			case DecimalInteger:
				{
				setState(1524);
				match(DecimalInteger);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_PropertyExpressionContext extends ParserRuleContext {
		public OC_AtomContext oC_Atom() {
			return getRuleContext(OC_AtomContext.class,0);
		}
		public List<OC_PropertyLookupContext> oC_PropertyLookup() {
			return getRuleContexts(OC_PropertyLookupContext.class);
		}
		public OC_PropertyLookupContext oC_PropertyLookup(int i) {
			return getRuleContext(OC_PropertyLookupContext.class,i);
		}
		public List<TerminalNode> SP() { return getTokens(CypherParser.SP); }
		public TerminalNode SP(int i) {
			return getToken(CypherParser.SP, i);
		}
		public OC_PropertyExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_PropertyExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_PropertyExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_PropertyExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_PropertyExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_PropertyExpressionContext oC_PropertyExpression() throws RecognitionException {
		OC_PropertyExpressionContext _localctx = new OC_PropertyExpressionContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_oC_PropertyExpression);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(1527);
			oC_Atom();
			setState(1532); 
			_errHandler.sync(this);
			_alt = 1;
			do {
				switch (_alt) {
				case 1:
					{
					{
					setState(1529);
					_errHandler.sync(this);
					_la = _input.LA(1);
					if (_la==SP) {
						{
						setState(1528);
						match(SP);
						}
					}

					setState(1531);
					oC_PropertyLookup();
					}
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				setState(1534); 
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,286,_ctx);
			} while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER );
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_PropertyKeyNameContext extends ParserRuleContext {
		public OC_SchemaNameContext oC_SchemaName() {
			return getRuleContext(OC_SchemaNameContext.class,0);
		}
		public OC_PropertyKeyNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_PropertyKeyName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_PropertyKeyName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_PropertyKeyName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_PropertyKeyName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_PropertyKeyNameContext oC_PropertyKeyName() throws RecognitionException {
		OC_PropertyKeyNameContext _localctx = new OC_PropertyKeyNameContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_oC_PropertyKeyName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1536);
			oC_SchemaName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_IntegerLiteralContext extends ParserRuleContext {
		public TerminalNode HexInteger() { return getToken(CypherParser.HexInteger, 0); }
		public TerminalNode OctalInteger() { return getToken(CypherParser.OctalInteger, 0); }
		public TerminalNode DecimalInteger() { return getToken(CypherParser.DecimalInteger, 0); }
		public OC_IntegerLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_IntegerLiteral; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_IntegerLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_IntegerLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_IntegerLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_IntegerLiteralContext oC_IntegerLiteral() throws RecognitionException {
		OC_IntegerLiteralContext _localctx = new OC_IntegerLiteralContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_oC_IntegerLiteral);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1538);
			_la = _input.LA(1);
			if ( !(((((_la - 99)) & ~0x3f) == 0 && ((1L << (_la - 99)) & ((1L << (HexInteger - 99)) | (1L << (DecimalInteger - 99)) | (1L << (OctalInteger - 99)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_DoubleLiteralContext extends ParserRuleContext {
		public TerminalNode ExponentDecimalReal() { return getToken(CypherParser.ExponentDecimalReal, 0); }
		public TerminalNode RegularDecimalReal() { return getToken(CypherParser.RegularDecimalReal, 0); }
		public OC_DoubleLiteralContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_DoubleLiteral; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_DoubleLiteral(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_DoubleLiteral(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_DoubleLiteral(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_DoubleLiteralContext oC_DoubleLiteral() throws RecognitionException {
		OC_DoubleLiteralContext _localctx = new OC_DoubleLiteralContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_oC_DoubleLiteral);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1540);
			_la = _input.LA(1);
			if ( !(_la==ExponentDecimalReal || _la==RegularDecimalReal) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_SchemaNameContext extends ParserRuleContext {
		public OC_SymbolicNameContext oC_SymbolicName() {
			return getRuleContext(OC_SymbolicNameContext.class,0);
		}
		public OC_ReservedWordContext oC_ReservedWord() {
			return getRuleContext(OC_ReservedWordContext.class,0);
		}
		public OC_SchemaNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_SchemaName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_SchemaName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_SchemaName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_SchemaName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_SchemaNameContext oC_SchemaName() throws RecognitionException {
		OC_SchemaNameContext _localctx = new OC_SchemaNameContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_oC_SchemaName);
		try {
			setState(1544);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case COUNT:
			case FILTER:
			case EXTRACT:
			case ANY:
			case NONE:
			case SINGLE:
			case HexLetter:
			case UnescapedSymbolicName:
			case EscapedSymbolicName:
				enterOuterAlt(_localctx, 1);
				{
				setState(1542);
				oC_SymbolicName();
				}
				break;
			case UNION:
			case ALL:
			case OPTIONAL:
			case MATCH:
			case UNWIND:
			case AS:
			case MERGE:
			case ON:
			case CREATE:
			case SET:
			case DETACH:
			case DELETE:
			case REMOVE:
			case WITH:
			case DISTINCT:
			case RETURN:
			case ORDER:
			case BY:
			case L_SKIP:
			case LIMIT:
			case ASCENDING:
			case ASC:
			case DESCENDING:
			case DESC:
			case WHERE:
			case OR:
			case XOR:
			case AND:
			case NOT:
			case IN:
			case STARTS:
			case ENDS:
			case CONTAINS:
			case IS:
			case NULL:
			case TRUE:
			case FALSE:
			case EXISTS:
			case CASE:
			case ELSE:
			case END:
			case WHEN:
			case THEN:
			case CONSTRAINT:
			case DO:
			case FOR:
			case REQUIRE:
			case UNIQUE:
			case MANDATORY:
			case SCALAR:
			case OF:
			case ADD:
			case DROP:
				enterOuterAlt(_localctx, 2);
				{
				setState(1543);
				oC_ReservedWord();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_ReservedWordContext extends ParserRuleContext {
		public TerminalNode ALL() { return getToken(CypherParser.ALL, 0); }
		public TerminalNode ASC() { return getToken(CypherParser.ASC, 0); }
		public TerminalNode ASCENDING() { return getToken(CypherParser.ASCENDING, 0); }
		public TerminalNode BY() { return getToken(CypherParser.BY, 0); }
		public TerminalNode CREATE() { return getToken(CypherParser.CREATE, 0); }
		public TerminalNode DELETE() { return getToken(CypherParser.DELETE, 0); }
		public TerminalNode DESC() { return getToken(CypherParser.DESC, 0); }
		public TerminalNode DESCENDING() { return getToken(CypherParser.DESCENDING, 0); }
		public TerminalNode DETACH() { return getToken(CypherParser.DETACH, 0); }
		public TerminalNode EXISTS() { return getToken(CypherParser.EXISTS, 0); }
		public TerminalNode LIMIT() { return getToken(CypherParser.LIMIT, 0); }
		public TerminalNode MATCH() { return getToken(CypherParser.MATCH, 0); }
		public TerminalNode MERGE() { return getToken(CypherParser.MERGE, 0); }
		public TerminalNode ON() { return getToken(CypherParser.ON, 0); }
		public TerminalNode OPTIONAL() { return getToken(CypherParser.OPTIONAL, 0); }
		public TerminalNode ORDER() { return getToken(CypherParser.ORDER, 0); }
		public TerminalNode REMOVE() { return getToken(CypherParser.REMOVE, 0); }
		public TerminalNode RETURN() { return getToken(CypherParser.RETURN, 0); }
		public TerminalNode SET() { return getToken(CypherParser.SET, 0); }
		public TerminalNode L_SKIP() { return getToken(CypherParser.L_SKIP, 0); }
		public TerminalNode WHERE() { return getToken(CypherParser.WHERE, 0); }
		public TerminalNode WITH() { return getToken(CypherParser.WITH, 0); }
		public TerminalNode UNION() { return getToken(CypherParser.UNION, 0); }
		public TerminalNode UNWIND() { return getToken(CypherParser.UNWIND, 0); }
		public TerminalNode AND() { return getToken(CypherParser.AND, 0); }
		public TerminalNode AS() { return getToken(CypherParser.AS, 0); }
		public TerminalNode CONTAINS() { return getToken(CypherParser.CONTAINS, 0); }
		public TerminalNode DISTINCT() { return getToken(CypherParser.DISTINCT, 0); }
		public TerminalNode ENDS() { return getToken(CypherParser.ENDS, 0); }
		public TerminalNode IN() { return getToken(CypherParser.IN, 0); }
		public TerminalNode IS() { return getToken(CypherParser.IS, 0); }
		public TerminalNode NOT() { return getToken(CypherParser.NOT, 0); }
		public TerminalNode OR() { return getToken(CypherParser.OR, 0); }
		public TerminalNode STARTS() { return getToken(CypherParser.STARTS, 0); }
		public TerminalNode XOR() { return getToken(CypherParser.XOR, 0); }
		public TerminalNode FALSE() { return getToken(CypherParser.FALSE, 0); }
		public TerminalNode TRUE() { return getToken(CypherParser.TRUE, 0); }
		public TerminalNode NULL() { return getToken(CypherParser.NULL, 0); }
		public TerminalNode CONSTRAINT() { return getToken(CypherParser.CONSTRAINT, 0); }
		public TerminalNode DO() { return getToken(CypherParser.DO, 0); }
		public TerminalNode FOR() { return getToken(CypherParser.FOR, 0); }
		public TerminalNode REQUIRE() { return getToken(CypherParser.REQUIRE, 0); }
		public TerminalNode UNIQUE() { return getToken(CypherParser.UNIQUE, 0); }
		public TerminalNode CASE() { return getToken(CypherParser.CASE, 0); }
		public TerminalNode WHEN() { return getToken(CypherParser.WHEN, 0); }
		public TerminalNode THEN() { return getToken(CypherParser.THEN, 0); }
		public TerminalNode ELSE() { return getToken(CypherParser.ELSE, 0); }
		public TerminalNode END() { return getToken(CypherParser.END, 0); }
		public TerminalNode MANDATORY() { return getToken(CypherParser.MANDATORY, 0); }
		public TerminalNode SCALAR() { return getToken(CypherParser.SCALAR, 0); }
		public TerminalNode OF() { return getToken(CypherParser.OF, 0); }
		public TerminalNode ADD() { return getToken(CypherParser.ADD, 0); }
		public TerminalNode DROP() { return getToken(CypherParser.DROP, 0); }
		public OC_ReservedWordContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_ReservedWord; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_ReservedWord(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_ReservedWord(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_ReservedWord(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_ReservedWordContext oC_ReservedWord() throws RecognitionException {
		OC_ReservedWordContext _localctx = new OC_ReservedWordContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_oC_ReservedWord);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1546);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << UNION) | (1L << ALL) | (1L << OPTIONAL) | (1L << MATCH) | (1L << UNWIND) | (1L << AS) | (1L << MERGE) | (1L << ON) | (1L << CREATE) | (1L << SET) | (1L << DETACH) | (1L << DELETE) | (1L << REMOVE) | (1L << WITH) | (1L << DISTINCT) | (1L << RETURN))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (ORDER - 64)) | (1L << (BY - 64)) | (1L << (L_SKIP - 64)) | (1L << (LIMIT - 64)) | (1L << (ASCENDING - 64)) | (1L << (ASC - 64)) | (1L << (DESCENDING - 64)) | (1L << (DESC - 64)) | (1L << (WHERE - 64)) | (1L << (OR - 64)) | (1L << (XOR - 64)) | (1L << (AND - 64)) | (1L << (NOT - 64)) | (1L << (IN - 64)) | (1L << (STARTS - 64)) | (1L << (ENDS - 64)) | (1L << (CONTAINS - 64)) | (1L << (IS - 64)) | (1L << (NULL - 64)) | (1L << (TRUE - 64)) | (1L << (FALSE - 64)) | (1L << (EXISTS - 64)) | (1L << (CASE - 64)) | (1L << (ELSE - 64)) | (1L << (END - 64)) | (1L << (WHEN - 64)) | (1L << (THEN - 64)) | (1L << (CONSTRAINT - 64)) | (1L << (DO - 64)) | (1L << (FOR - 64)) | (1L << (REQUIRE - 64)) | (1L << (UNIQUE - 64)) | (1L << (MANDATORY - 64)) | (1L << (SCALAR - 64)) | (1L << (OF - 64)) | (1L << (ADD - 64)) | (1L << (DROP - 64)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_SymbolicNameContext extends ParserRuleContext {
		public TerminalNode UnescapedSymbolicName() { return getToken(CypherParser.UnescapedSymbolicName, 0); }
		public TerminalNode EscapedSymbolicName() { return getToken(CypherParser.EscapedSymbolicName, 0); }
		public TerminalNode HexLetter() { return getToken(CypherParser.HexLetter, 0); }
		public TerminalNode COUNT() { return getToken(CypherParser.COUNT, 0); }
		public TerminalNode FILTER() { return getToken(CypherParser.FILTER, 0); }
		public TerminalNode EXTRACT() { return getToken(CypherParser.EXTRACT, 0); }
		public TerminalNode ANY() { return getToken(CypherParser.ANY, 0); }
		public TerminalNode NONE() { return getToken(CypherParser.NONE, 0); }
		public TerminalNode SINGLE() { return getToken(CypherParser.SINGLE, 0); }
		public OC_SymbolicNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_SymbolicName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_SymbolicName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_SymbolicName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_SymbolicName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_SymbolicNameContext oC_SymbolicName() throws RecognitionException {
		OC_SymbolicNameContext _localctx = new OC_SymbolicNameContext(_ctx, getState());
		enterRule(_localctx, 184, RULE_oC_SymbolicName);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1548);
			_la = _input.LA(1);
			if ( !(((((_la - 83)) & ~0x3f) == 0 && ((1L << (_la - 83)) & ((1L << (COUNT - 83)) | (1L << (FILTER - 83)) | (1L << (EXTRACT - 83)) | (1L << (ANY - 83)) | (1L << (NONE - 83)) | (1L << (SINGLE - 83)) | (1L << (HexLetter - 83)) | (1L << (UnescapedSymbolicName - 83)) | (1L << (EscapedSymbolicName - 83)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_LeftArrowHeadContext extends ParserRuleContext {
		public OC_LeftArrowHeadContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_LeftArrowHead; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_LeftArrowHead(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_LeftArrowHead(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_LeftArrowHead(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_LeftArrowHeadContext oC_LeftArrowHead() throws RecognitionException {
		OC_LeftArrowHeadContext _localctx = new OC_LeftArrowHeadContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_oC_LeftArrowHead);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1550);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__18) | (1L << T__26) | (1L << T__27) | (1L << T__28) | (1L << T__29))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_RightArrowHeadContext extends ParserRuleContext {
		public OC_RightArrowHeadContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_RightArrowHead; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_RightArrowHead(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_RightArrowHead(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_RightArrowHead(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_RightArrowHeadContext oC_RightArrowHead() throws RecognitionException {
		OC_RightArrowHeadContext _localctx = new OC_RightArrowHeadContext(_ctx, getState());
		enterRule(_localctx, 188, RULE_oC_RightArrowHead);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1552);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__19) | (1L << T__30) | (1L << T__31) | (1L << T__32) | (1L << T__33))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OC_DashContext extends ParserRuleContext {
		public OC_DashContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_oC_Dash; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).enterOC_Dash(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CypherListener ) ((CypherListener)listener).exitOC_Dash(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CypherVisitor ) return ((CypherVisitor<? extends T>)visitor).visitOC_Dash(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OC_DashContext oC_Dash() throws RecognitionException {
		OC_DashContext _localctx = new OC_DashContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_oC_Dash);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1554);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__4) | (1L << T__34) | (1L << T__35) | (1L << T__36) | (1L << T__37) | (1L << T__38) | (1L << T__39) | (1L << T__40) | (1L << T__41) | (1L << T__42) | (1L << T__43) | (1L << T__44))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\u0081\u0617\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
		"\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\tT"+
		"\4U\tU\4V\tV\4W\tW\4X\tX\4Y\tY\4Z\tZ\4[\t[\4\\\t\\\4]\t]\4^\t^\4_\t_\4"+
		"`\t`\4a\ta\3\2\5\2\u00c4\n\2\3\2\3\2\5\2\u00c8\n\2\3\2\5\2\u00cb\n\2\3"+
		"\2\5\2\u00ce\n\2\3\2\3\2\3\3\3\3\3\4\3\4\5\4\u00d6\n\4\3\5\3\5\5\5\u00da"+
		"\n\5\3\5\7\5\u00dd\n\5\f\5\16\5\u00e0\13\5\3\6\3\6\3\6\3\6\5\6\u00e6\n"+
		"\6\3\6\3\6\3\6\5\6\u00eb\n\6\3\6\5\6\u00ee\n\6\3\7\3\7\5\7\u00f2\n\7\3"+
		"\b\3\b\5\b\u00f6\n\b\7\b\u00f8\n\b\f\b\16\b\u00fb\13\b\3\b\3\b\3\b\5\b"+
		"\u0100\n\b\7\b\u0102\n\b\f\b\16\b\u0105\13\b\3\b\3\b\5\b\u0109\n\b\3\b"+
		"\7\b\u010c\n\b\f\b\16\b\u010f\13\b\3\b\5\b\u0112\n\b\3\b\5\b\u0115\n\b"+
		"\5\b\u0117\n\b\3\t\3\t\5\t\u011b\n\t\7\t\u011d\n\t\f\t\16\t\u0120\13\t"+
		"\3\t\3\t\5\t\u0124\n\t\7\t\u0126\n\t\f\t\16\t\u0129\13\t\3\t\3\t\5\t\u012d"+
		"\n\t\6\t\u012f\n\t\r\t\16\t\u0130\3\t\3\t\3\n\3\n\3\n\3\n\3\n\5\n\u013a"+
		"\n\n\3\13\3\13\3\13\5\13\u013f\n\13\3\f\3\f\5\f\u0143\n\f\3\f\3\f\5\f"+
		"\u0147\n\f\3\f\3\f\5\f\u014b\n\f\3\f\5\f\u014e\n\f\3\r\3\r\5\r\u0152\n"+
		"\r\3\r\3\r\3\r\3\r\3\r\3\r\3\16\3\16\5\16\u015c\n\16\3\16\3\16\3\16\7"+
		"\16\u0161\n\16\f\16\16\16\u0164\13\16\3\17\3\17\3\17\3\17\3\17\3\17\3"+
		"\17\3\17\3\17\3\17\5\17\u0170\n\17\3\20\3\20\5\20\u0174\n\20\3\20\3\20"+
		"\3\21\3\21\5\21\u017a\n\21\3\21\3\21\3\21\7\21\u017f\n\21\f\21\16\21\u0182"+
		"\13\21\3\22\3\22\5\22\u0186\n\22\3\22\3\22\5\22\u018a\n\22\3\22\3\22\3"+
		"\22\3\22\5\22\u0190\n\22\3\22\3\22\5\22\u0194\n\22\3\22\3\22\3\22\3\22"+
		"\5\22\u019a\n\22\3\22\3\22\5\22\u019e\n\22\3\22\3\22\3\22\3\22\5\22\u01a4"+
		"\n\22\3\22\3\22\5\22\u01a8\n\22\3\23\3\23\5\23\u01ac\n\23\3\23\3\23\5"+
		"\23\u01b0\n\23\3\23\3\23\5\23\u01b4\n\23\3\23\3\23\5\23\u01b8\n\23\3\23"+
		"\7\23\u01bb\n\23\f\23\16\23\u01be\13\23\3\24\3\24\3\24\3\24\5\24\u01c4"+
		"\n\24\3\24\3\24\5\24\u01c8\n\24\3\24\7\24\u01cb\n\24\f\24\16\24\u01ce"+
		"\13\24\3\25\3\25\3\25\3\25\5\25\u01d4\n\25\3\26\3\26\3\26\3\26\5\26\u01da"+
		"\n\26\3\26\3\26\3\26\5\26\u01df\n\26\3\27\3\27\3\27\3\27\5\27\u01e5\n"+
		"\27\3\27\3\27\3\27\3\27\5\27\u01eb\n\27\3\30\3\30\5\30\u01ef\n\30\3\30"+
		"\3\30\5\30\u01f3\n\30\3\30\7\30\u01f6\n\30\f\30\16\30\u01f9\13\30\3\30"+
		"\5\30\u01fc\n\30\3\31\3\31\3\31\3\31\3\31\5\31\u0203\n\31\3\31\3\31\3"+
		"\32\3\32\5\32\u0209\n\32\3\32\5\32\u020c\n\32\3\32\3\32\3\32\5\32\u0211"+
		"\n\32\3\32\5\32\u0214\n\32\3\33\3\33\5\33\u0218\n\33\3\33\5\33\u021b\n"+
		"\33\3\33\3\33\3\33\3\34\3\34\3\34\5\34\u0223\n\34\3\34\3\34\5\34\u0227"+
		"\n\34\3\34\3\34\5\34\u022b\n\34\3\35\3\35\5\35\u022f\n\35\3\35\3\35\5"+
		"\35\u0233\n\35\3\35\7\35\u0236\n\35\f\35\16\35\u0239\13\35\3\35\3\35\5"+
		"\35\u023d\n\35\3\35\3\35\5\35\u0241\n\35\3\35\7\35\u0244\n\35\f\35\16"+
		"\35\u0247\13\35\5\35\u0249\n\35\3\36\3\36\3\36\3\36\3\36\3\36\3\36\5\36"+
		"\u0252\n\36\3\37\3\37\3\37\3\37\3\37\3\37\3\37\5\37\u025b\n\37\3\37\7"+
		"\37\u025e\n\37\f\37\16\37\u0261\13\37\3 \3 \3 \3 \3!\3!\3!\3!\3\"\3\""+
		"\5\"\u026d\n\"\3\"\5\"\u0270\n\"\3#\3#\3#\3#\3$\3$\5$\u0278\n$\3$\3$\5"+
		"$\u027c\n$\3$\7$\u027f\n$\f$\16$\u0282\13$\3%\3%\5%\u0286\n%\3%\3%\5%"+
		"\u028a\n%\3%\3%\3%\5%\u028f\n%\3&\3&\3\'\3\'\5\'\u0295\n\'\3\'\7\'\u0298"+
		"\n\'\f\'\16\'\u029b\13\'\3\'\3\'\3\'\3\'\5\'\u02a1\n\'\3(\3(\5(\u02a5"+
		"\n(\3(\3(\5(\u02a9\n(\5(\u02ab\n(\3(\3(\5(\u02af\n(\5(\u02b1\n(\3(\3("+
		"\5(\u02b5\n(\5(\u02b7\n(\3(\3(\3)\3)\5)\u02bd\n)\3)\3)\3*\3*\5*\u02c3"+
		"\n*\3*\3*\5*\u02c7\n*\3*\5*\u02ca\n*\3*\5*\u02cd\n*\3*\3*\5*\u02d1\n*"+
		"\3*\3*\3*\3*\5*\u02d7\n*\3*\3*\5*\u02db\n*\3*\5*\u02de\n*\3*\5*\u02e1"+
		"\n*\3*\3*\3*\3*\5*\u02e7\n*\3*\5*\u02ea\n*\3*\5*\u02ed\n*\3*\3*\5*\u02f1"+
		"\n*\3*\3*\3*\3*\5*\u02f7\n*\3*\5*\u02fa\n*\3*\5*\u02fd\n*\3*\3*\5*\u0301"+
		"\n*\3+\3+\5+\u0305\n+\3+\3+\5+\u0309\n+\5+\u030b\n+\3+\3+\5+\u030f\n+"+
		"\5+\u0311\n+\3+\5+\u0314\n+\3+\3+\5+\u0318\n+\5+\u031a\n+\3+\3+\3,\3,"+
		"\5,\u0320\n,\3-\3-\5-\u0324\n-\3-\3-\5-\u0328\n-\3-\3-\5-\u032c\n-\3-"+
		"\5-\u032f\n-\3-\7-\u0332\n-\f-\16-\u0335\13-\3.\3.\5.\u0339\n.\3.\7.\u033c"+
		"\n.\f.\16.\u033f\13.\3/\3/\5/\u0343\n/\3/\3/\3\60\3\60\5\60\u0349\n\60"+
		"\3\60\3\60\5\60\u034d\n\60\5\60\u034f\n\60\3\60\3\60\5\60\u0353\n\60\3"+
		"\60\3\60\5\60\u0357\n\60\5\60\u0359\n\60\5\60\u035b\n\60\3\61\3\61\3\62"+
		"\3\62\3\63\3\63\3\64\3\64\3\64\3\64\3\64\7\64\u0368\n\64\f\64\16\64\u036b"+
		"\13\64\3\65\3\65\3\65\3\65\3\65\7\65\u0372\n\65\f\65\16\65\u0375\13\65"+
		"\3\66\3\66\3\66\3\66\3\66\7\66\u037c\n\66\f\66\16\66\u037f\13\66\3\67"+
		"\3\67\5\67\u0383\n\67\7\67\u0385\n\67\f\67\16\67\u0388\13\67\3\67\3\67"+
		"\38\38\58\u038e\n8\38\78\u0391\n8\f8\168\u0394\138\39\39\59\u0398\n9\3"+
		"9\39\59\u039c\n9\39\39\59\u03a0\n9\39\39\59\u03a4\n9\39\79\u03a7\n9\f"+
		"9\169\u03aa\139\3:\3:\5:\u03ae\n:\3:\3:\5:\u03b2\n:\3:\3:\5:\u03b6\n:"+
		"\3:\3:\5:\u03ba\n:\3:\3:\5:\u03be\n:\3:\3:\5:\u03c2\n:\3:\7:\u03c5\n:"+
		"\f:\16:\u03c8\13:\3;\3;\5;\u03cc\n;\3;\3;\5;\u03d0\n;\3;\7;\u03d3\n;\f"+
		";\16;\u03d6\13;\3<\3<\5<\u03da\n<\7<\u03dc\n<\f<\16<\u03df\13<\3<\3<\3"+
		"=\3=\5=\u03e5\n=\3=\3=\3=\3=\3=\5=\u03ec\n=\3=\3=\5=\u03f0\n=\3=\3=\5"+
		"=\u03f4\n=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\5=\u0403\n=\3=\5=\u0406"+
		"\n=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\3=\7=\u0413\n=\f=\16=\u0416\13=\3>\3"+
		">\5>\u041a\n>\3>\7>\u041d\n>\f>\16>\u0420\13>\3>\5>\u0423\n>\3>\5>\u0426"+
		"\n>\3?\3?\3?\3?\3?\5?\u042d\n?\3?\3?\5?\u0431\n?\3?\3?\5?\u0435\n?\3?"+
		"\3?\3?\3?\3?\5?\u043c\n?\3?\3?\5?\u0440\n?\3?\3?\5?\u0444\n?\3?\3?\3?"+
		"\3?\5?\u044a\n?\3?\3?\5?\u044e\n?\3?\3?\5?\u0452\n?\3?\5?\u0455\n?\3?"+
		"\3?\5?\u0459\n?\3?\3?\3?\3?\5?\u045f\n?\3?\3?\5?\u0463\n?\3?\3?\5?\u0467"+
		"\n?\3?\3?\3?\3?\5?\u046d\n?\3?\3?\5?\u0471\n?\3?\3?\5?\u0475\n?\3?\3?"+
		"\3?\3?\5?\u047b\n?\3?\3?\5?\u047f\n?\3?\3?\5?\u0483\n?\3?\3?\3?\3?\5?"+
		"\u0489\n?\3?\3?\5?\u048d\n?\3?\3?\5?\u0491\n?\3?\3?\3?\3?\3?\3?\5?\u0499"+
		"\n?\3@\3@\3@\3@\3@\3@\5@\u04a1\n@\3A\3A\3B\3B\5B\u04a7\nB\3B\3B\5B\u04ab"+
		"\nB\3B\3B\5B\u04af\nB\3B\3B\5B\u04b3\nB\7B\u04b5\nB\fB\16B\u04b8\13B\5"+
		"B\u04ba\nB\3B\3B\3C\3C\5C\u04c0\nC\3C\3C\3C\5C\u04c5\nC\3C\3C\3C\5C\u04ca"+
		"\nC\3C\3C\3C\5C\u04cf\nC\3C\3C\3C\5C\u04d4\nC\3C\3C\3C\5C\u04d9\nC\3C"+
		"\5C\u04dc\nC\3D\3D\5D\u04e0\nD\3D\3D\5D\u04e4\nD\3D\3D\3E\3E\5E\u04ea"+
		"\nE\3E\6E\u04ed\nE\rE\16E\u04ee\3F\3F\5F\u04f3\nF\3F\5F\u04f6\nF\3G\3"+
		"G\3G\3G\3G\3G\3H\3H\5H\u0500\nH\3H\3H\5H\u0504\nH\3H\3H\5H\u0508\nH\5"+
		"H\u050a\nH\3H\3H\5H\u050e\nH\3H\3H\5H\u0512\nH\3H\3H\5H\u0516\nH\7H\u0518"+
		"\nH\fH\16H\u051b\13H\5H\u051d\nH\3H\3H\3I\3I\5I\u0523\nI\3J\3J\5J\u0527"+
		"\nJ\3J\3J\5J\u052b\nJ\3J\3J\5J\u052f\nJ\3J\3J\5J\u0533\nJ\3J\3J\5J\u0537"+
		"\nJ\7J\u0539\nJ\fJ\16J\u053c\13J\5J\u053e\nJ\3J\3J\3K\3K\3L\3L\3M\3M\3"+
		"M\3N\3N\3N\7N\u054c\nN\fN\16N\u054f\13N\3O\3O\5O\u0553\nO\3O\3O\5O\u0557"+
		"\nO\3O\3O\5O\u055b\nO\3O\5O\u055e\nO\3O\5O\u0561\nO\3O\3O\3P\3P\5P\u0567"+
		"\nP\3P\3P\5P\u056b\nP\3P\3P\5P\u056f\nP\5P\u0571\nP\3P\3P\5P\u0575\nP"+
		"\3P\3P\5P\u0579\nP\3P\3P\5P\u057d\nP\5P\u057f\nP\3P\3P\5P\u0583\nP\3P"+
		"\3P\5P\u0587\nP\3P\3P\3Q\3Q\5Q\u058d\nQ\3Q\3Q\3R\3R\5R\u0593\nR\3R\6R"+
		"\u0596\nR\rR\16R\u0597\3R\3R\5R\u059c\nR\3R\3R\5R\u05a0\nR\3R\6R\u05a3"+
		"\nR\rR\16R\u05a4\5R\u05a7\nR\3R\5R\u05aa\nR\3R\3R\5R\u05ae\nR\3R\5R\u05b1"+
		"\nR\3R\5R\u05b4\nR\3R\3R\3S\3S\5S\u05ba\nS\3S\3S\5S\u05be\nS\3S\3S\5S"+
		"\u05c2\nS\3S\3S\3T\3T\3U\3U\5U\u05ca\nU\3V\3V\5V\u05ce\nV\3V\3V\5V\u05d2"+
		"\nV\3V\3V\5V\u05d6\nV\3V\3V\5V\u05da\nV\3V\3V\5V\u05de\nV\3V\3V\5V\u05e2"+
		"\nV\3V\3V\5V\u05e6\nV\3V\3V\5V\u05ea\nV\7V\u05ec\nV\fV\16V\u05ef\13V\5"+
		"V\u05f1\nV\3V\3V\3W\3W\3W\5W\u05f8\nW\3X\3X\5X\u05fc\nX\3X\6X\u05ff\n"+
		"X\rX\16X\u0600\3Y\3Y\3Z\3Z\3[\3[\3\\\3\\\5\\\u060b\n\\\3]\3]\3^\3^\3_"+
		"\3_\3`\3`\3a\3a\3a\2\2b\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36 \"$&(*"+
		",.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082\u0084"+
		"\u0086\u0088\u008a\u008c\u008e\u0090\u0092\u0094\u0096\u0098\u009a\u009c"+
		"\u009e\u00a0\u00a2\u00a4\u00a6\u00a8\u00aa\u00ac\u00ae\u00b0\u00b2\u00b4"+
		"\u00b6\u00b8\u00ba\u00bc\u00be\u00c0\2\f\3\2FI\4\2\7\7\20\20\3\2[\\\3"+
		"\2eg\3\2op\6\2\60<?T[bqz\6\2UZhh{{~~\4\2\25\25\35 \4\2\26\26!$\4\2\7\7"+
		"%/\2\u06fa\2\u00c3\3\2\2\2\4\u00d1\3\2\2\2\6\u00d5\3\2\2\2\b\u00d7\3\2"+
		"\2\2\n\u00ed\3\2\2\2\f\u00f1\3\2\2\2\16\u0116\3\2\2\2\20\u012e\3\2\2\2"+
		"\22\u0139\3\2\2\2\24\u013e\3\2\2\2\26\u0142\3\2\2\2\30\u014f\3\2\2\2\32"+
		"\u0159\3\2\2\2\34\u016f\3\2\2\2\36\u0171\3\2\2\2 \u0177\3\2\2\2\"\u01a7"+
		"\3\2\2\2$\u01ab\3\2\2\2&\u01bf\3\2\2\2(\u01d3\3\2\2\2*\u01d5\3\2\2\2,"+
		"\u01e0\3\2\2\2.\u01fb\3\2\2\2\60\u0202\3\2\2\2\62\u0206\3\2\2\2\64\u0215"+
		"\3\2\2\2\66\u021f\3\2\2\28\u0248\3\2\2\2:\u0251\3\2\2\2<\u0253\3\2\2\2"+
		">\u0262\3\2\2\2@\u0266\3\2\2\2B\u026a\3\2\2\2D\u0271\3\2\2\2F\u0275\3"+
		"\2\2\2H\u028e\3\2\2\2J\u0290\3\2\2\2L\u02a0\3\2\2\2N\u02a2\3\2\2\2P\u02ba"+
		"\3\2\2\2R\u0300\3\2\2\2T\u0302\3\2\2\2V\u031f\3\2\2\2X\u0321\3\2\2\2Z"+
		"\u0336\3\2\2\2\\\u0340\3\2\2\2^\u0346\3\2\2\2`\u035c\3\2\2\2b\u035e\3"+
		"\2\2\2d\u0360\3\2\2\2f\u0362\3\2\2\2h\u036c\3\2\2\2j\u0376\3\2\2\2l\u0386"+
		"\3\2\2\2n\u038b\3\2\2\2p\u0395\3\2\2\2r\u03ab\3\2\2\2t\u03c9\3\2\2\2v"+
		"\u03dd\3\2\2\2x\u03e2\3\2\2\2z\u0417\3\2\2\2|\u0498\3\2\2\2~\u04a0\3\2"+
		"\2\2\u0080\u04a2\3\2\2\2\u0082\u04a4\3\2\2\2\u0084\u04db\3\2\2\2\u0086"+
		"\u04dd\3\2\2\2\u0088\u04e7\3\2\2\2\u008a\u04f0\3\2\2\2\u008c\u04f7\3\2"+
		"\2\2\u008e\u04fd\3\2\2\2\u0090\u0522\3\2\2\2\u0092\u0524\3\2\2\2\u0094"+
		"\u0541\3\2\2\2\u0096\u0543\3\2\2\2\u0098\u0545\3\2\2\2\u009a\u054d\3\2"+
		"\2\2\u009c\u0550\3\2\2\2\u009e\u0564\3\2\2\2\u00a0\u058a\3\2\2\2\u00a2"+
		"\u05a6\3\2\2\2\u00a4\u05b7\3\2\2\2\u00a6\u05c5\3\2\2\2\u00a8\u05c9\3\2"+
		"\2\2\u00aa\u05cb\3\2\2\2\u00ac\u05f4\3\2\2\2\u00ae\u05f9\3\2\2\2\u00b0"+
		"\u0602\3\2\2\2\u00b2\u0604\3\2\2\2\u00b4\u0606\3\2\2\2\u00b6\u060a\3\2"+
		"\2\2\u00b8\u060c\3\2\2\2\u00ba\u060e\3\2\2\2\u00bc\u0610\3\2\2\2\u00be"+
		"\u0612\3\2\2\2\u00c0\u0614\3\2\2\2\u00c2\u00c4\7\177\2\2\u00c3\u00c2\3"+
		"\2\2\2\u00c3\u00c4\3\2\2\2\u00c4\u00c5\3\2\2\2\u00c5\u00ca\5\4\3\2\u00c6"+
		"\u00c8\7\177\2\2\u00c7\u00c6\3\2\2\2\u00c7\u00c8\3\2\2\2\u00c8\u00c9\3"+
		"\2\2\2\u00c9\u00cb\7\3\2\2\u00ca\u00c7\3\2\2\2\u00ca\u00cb\3\2\2\2\u00cb"+
		"\u00cd\3\2\2\2\u00cc\u00ce\7\177\2\2\u00cd\u00cc\3\2\2\2\u00cd\u00ce\3"+
		"\2\2\2\u00ce\u00cf\3\2\2\2\u00cf\u00d0\7\2\2\3\u00d0\3\3\2\2\2\u00d1\u00d2"+
		"\5\6\4\2\u00d2\5\3\2\2\2\u00d3\u00d6\5\b\5\2\u00d4\u00d6\5,\27\2\u00d5"+
		"\u00d3\3\2\2\2\u00d5\u00d4\3\2\2\2\u00d6\7\3\2\2\2\u00d7\u00de\5\f\7\2"+
		"\u00d8\u00da\7\177\2\2\u00d9\u00d8\3\2\2\2\u00d9\u00da\3\2\2\2\u00da\u00db"+
		"\3\2\2\2\u00db\u00dd\5\n\6\2\u00dc\u00d9\3\2\2\2\u00dd\u00e0\3\2\2\2\u00de"+
		"\u00dc\3\2\2\2\u00de\u00df\3\2\2\2\u00df\t\3\2\2\2\u00e0\u00de\3\2\2\2"+
		"\u00e1\u00e2\7\60\2\2\u00e2\u00e3\7\177\2\2\u00e3\u00e5\7\61\2\2\u00e4"+
		"\u00e6\7\177\2\2\u00e5\u00e4\3\2\2\2\u00e5\u00e6\3\2\2\2\u00e6\u00e7\3"+
		"\2\2\2\u00e7\u00ee\5\f\7\2\u00e8\u00ea\7\60\2\2\u00e9\u00eb\7\177\2\2"+
		"\u00ea\u00e9\3\2\2\2\u00ea\u00eb\3\2\2\2\u00eb\u00ec\3\2\2\2\u00ec\u00ee"+
		"\5\f\7\2\u00ed\u00e1\3\2\2\2\u00ed\u00e8\3\2\2\2\u00ee\13\3\2\2\2\u00ef"+
		"\u00f2\5\16\b\2\u00f0\u00f2\5\20\t\2\u00f1\u00ef\3\2\2\2\u00f1\u00f0\3"+
		"\2\2\2\u00f2\r\3\2\2\2\u00f3\u00f5\5\24\13\2\u00f4\u00f6\7\177\2\2\u00f5"+
		"\u00f4\3\2\2\2\u00f5\u00f6\3\2\2\2\u00f6\u00f8\3\2\2\2\u00f7\u00f3\3\2"+
		"\2\2\u00f8\u00fb\3\2\2\2\u00f9\u00f7\3\2\2\2\u00f9\u00fa\3\2\2\2\u00fa"+
		"\u00fc\3\2\2\2\u00fb\u00f9\3\2\2\2\u00fc\u0117\5\64\33\2\u00fd\u00ff\5"+
		"\24\13\2\u00fe\u0100\7\177\2\2\u00ff\u00fe\3\2\2\2\u00ff\u0100\3\2\2\2"+
		"\u0100\u0102\3\2\2\2\u0101\u00fd\3\2\2\2\u0102\u0105\3\2\2\2\u0103\u0101"+
		"\3\2\2\2\u0103\u0104\3\2\2\2\u0104\u0106\3\2\2\2\u0105\u0103\3\2\2\2\u0106"+
		"\u010d\5\22\n\2\u0107\u0109\7\177\2\2\u0108\u0107\3\2\2\2\u0108\u0109"+
		"\3\2\2\2\u0109\u010a\3\2\2\2\u010a\u010c\5\22\n\2\u010b\u0108\3\2\2\2"+
		"\u010c\u010f\3\2\2\2\u010d\u010b\3\2\2\2\u010d\u010e\3\2\2\2\u010e\u0114"+
		"\3\2\2\2\u010f\u010d\3\2\2\2\u0110\u0112\7\177\2\2\u0111\u0110\3\2\2\2"+
		"\u0111\u0112\3\2\2\2\u0112\u0113\3\2\2\2\u0113\u0115\5\64\33\2\u0114\u0111"+
		"\3\2\2\2\u0114\u0115\3\2\2\2\u0115\u0117\3\2\2\2\u0116\u00f9\3\2\2\2\u0116"+
		"\u0103\3\2\2\2\u0117\17\3\2\2\2\u0118\u011a\5\24\13\2\u0119\u011b\7\177"+
		"\2\2\u011a\u0119\3\2\2\2\u011a\u011b\3\2\2\2\u011b\u011d\3\2\2\2\u011c"+
		"\u0118\3\2\2\2\u011d\u0120\3\2\2\2\u011e\u011c\3\2\2\2\u011e\u011f\3\2"+
		"\2\2\u011f\u0127\3\2\2\2\u0120\u011e\3\2\2\2\u0121\u0123\5\22\n\2\u0122"+
		"\u0124\7\177\2\2\u0123\u0122\3\2\2\2\u0123\u0124\3\2\2\2\u0124\u0126\3"+
		"\2\2\2\u0125\u0121\3\2\2\2\u0126\u0129\3\2\2\2\u0127\u0125\3\2\2\2\u0127"+
		"\u0128\3\2\2\2\u0128\u012a\3\2\2\2\u0129\u0127\3\2\2\2\u012a\u012c\5\62"+
		"\32\2\u012b\u012d\7\177\2\2\u012c\u012b\3\2\2\2\u012c\u012d\3\2\2\2\u012d"+
		"\u012f\3\2\2\2\u012e\u011e\3\2\2\2\u012f\u0130\3\2\2\2\u0130\u012e\3\2"+
		"\2\2\u0130\u0131\3\2\2\2\u0131\u0132\3\2\2\2\u0132\u0133\5\16\b\2\u0133"+
		"\21\3\2\2\2\u0134\u013a\5\36\20\2\u0135\u013a\5\32\16\2\u0136\u013a\5"+
		"$\23\2\u0137\u013a\5 \21\2\u0138\u013a\5&\24\2\u0139\u0134\3\2\2\2\u0139"+
		"\u0135\3\2\2\2\u0139\u0136\3\2\2\2\u0139\u0137\3\2\2\2\u0139\u0138\3\2"+
		"\2\2\u013a\23\3\2\2\2\u013b\u013f\5\26\f\2\u013c\u013f\5\30\r\2\u013d"+
		"\u013f\5*\26\2\u013e\u013b\3\2\2\2\u013e\u013c\3\2\2\2\u013e\u013d\3\2"+
		"\2\2\u013f\25\3\2\2\2\u0140\u0141\7\62\2\2\u0141\u0143\7\177\2\2\u0142"+
		"\u0140\3\2\2\2\u0142\u0143\3\2\2\2\u0143\u0144\3\2\2\2\u0144\u0146\7\63"+
		"\2\2\u0145\u0147\7\177\2\2\u0146\u0145\3\2\2\2\u0146\u0147\3\2\2\2\u0147"+
		"\u0148\3\2\2\2\u0148\u014d\5F$\2\u0149\u014b\7\177\2\2\u014a\u0149\3\2"+
		"\2\2\u014a\u014b\3\2\2\2\u014b\u014c\3\2\2\2\u014c\u014e\5D#\2\u014d\u014a"+
		"\3\2\2\2\u014d\u014e\3\2\2\2\u014e\27\3\2\2\2\u014f\u0151\7\64\2\2\u0150"+
		"\u0152\7\177\2\2\u0151\u0150\3\2\2\2\u0151\u0152\3\2\2\2\u0152\u0153\3"+
		"\2\2\2\u0153\u0154\5d\63\2\u0154\u0155\7\177\2\2\u0155\u0156\7\65\2\2"+
		"\u0156\u0157\7\177\2\2\u0157\u0158\5\u00a6T\2\u0158\31\3\2\2\2\u0159\u015b"+
		"\7\66\2\2\u015a\u015c\7\177\2\2\u015b\u015a\3\2\2\2\u015b\u015c\3\2\2"+
		"\2\u015c\u015d\3\2\2\2\u015d\u0162\5H%\2\u015e\u015f\7\177\2\2\u015f\u0161"+
		"\5\34\17\2\u0160\u015e\3\2\2\2\u0161\u0164\3\2\2\2\u0162\u0160\3\2\2\2"+
		"\u0162\u0163\3\2\2\2\u0163\33\3\2\2\2\u0164\u0162\3\2\2\2\u0165\u0166"+
		"\7\67\2\2\u0166\u0167\7\177\2\2\u0167\u0168\7\63\2\2\u0168\u0169\7\177"+
		"\2\2\u0169\u0170\5 \21\2\u016a\u016b\7\67\2\2\u016b\u016c\7\177\2\2\u016c"+
		"\u016d\78\2\2\u016d\u016e\7\177\2\2\u016e\u0170\5 \21\2\u016f\u0165\3"+
		"\2\2\2\u016f\u016a\3\2\2\2\u0170\35\3\2\2\2\u0171\u0173\78\2\2\u0172\u0174"+
		"\7\177\2\2\u0173\u0172\3\2\2\2\u0173\u0174\3\2\2\2\u0174\u0175\3\2\2\2"+
		"\u0175\u0176\5F$\2\u0176\37\3\2\2\2\u0177\u0179\79\2\2\u0178\u017a\7\177"+
		"\2\2\u0179\u0178\3\2\2\2\u0179\u017a\3\2\2\2\u017a\u017b\3\2\2\2\u017b"+
		"\u0180\5\"\22\2\u017c\u017d\7\4\2\2\u017d\u017f\5\"\22\2\u017e\u017c\3"+
		"\2\2\2\u017f\u0182\3\2\2\2\u0180\u017e\3\2\2\2\u0180\u0181\3\2\2\2\u0181"+
		"!\3\2\2\2\u0182\u0180\3\2\2\2\u0183\u0185\5\u00aeX\2\u0184\u0186\7\177"+
		"\2\2\u0185\u0184\3\2\2\2\u0185\u0186\3\2\2\2\u0186\u0187\3\2\2\2\u0187"+
		"\u0189\7\5\2\2\u0188\u018a\7\177\2\2\u0189\u0188\3\2\2\2\u0189\u018a\3"+
		"\2\2\2\u018a\u018b\3\2\2\2\u018b\u018c\5d\63\2\u018c\u01a8\3\2\2\2\u018d"+
		"\u018f\5\u00a6T\2\u018e\u0190\7\177\2\2\u018f\u018e\3\2\2\2\u018f\u0190"+
		"\3\2\2\2\u0190\u0191\3\2\2\2\u0191\u0193\7\5\2\2\u0192\u0194\7\177\2\2"+
		"\u0193\u0192\3\2\2\2\u0193\u0194\3\2\2\2\u0194\u0195\3\2\2\2\u0195\u0196"+
		"\5d\63\2\u0196\u01a8\3\2\2\2\u0197\u0199\5\u00a6T\2\u0198\u019a\7\177"+
		"\2\2\u0199\u0198\3\2\2\2\u0199\u019a\3\2\2\2\u019a\u019b\3\2\2\2\u019b"+
		"\u019d\7\6\2\2\u019c\u019e\7\177\2\2\u019d\u019c\3\2\2\2\u019d\u019e\3"+
		"\2\2\2\u019e\u019f\3\2\2\2\u019f\u01a0\5d\63\2\u01a0\u01a8\3\2\2\2\u01a1"+
		"\u01a3\5\u00a6T\2\u01a2\u01a4\7\177\2\2\u01a3\u01a2\3\2\2\2\u01a3\u01a4"+
		"\3\2\2\2\u01a4\u01a5\3\2\2\2\u01a5\u01a6\5Z.\2\u01a6\u01a8\3\2\2\2\u01a7"+
		"\u0183\3\2\2\2\u01a7\u018d\3\2\2\2\u01a7\u0197\3\2\2\2\u01a7\u01a1\3\2"+
		"\2\2\u01a8#\3\2\2\2\u01a9\u01aa\7:\2\2\u01aa\u01ac\7\177\2\2\u01ab\u01a9"+
		"\3\2\2\2\u01ab\u01ac\3\2\2\2\u01ac\u01ad\3\2\2\2\u01ad\u01af\7;\2\2\u01ae"+
		"\u01b0\7\177\2\2\u01af\u01ae\3\2\2\2\u01af\u01b0\3\2\2\2\u01b0\u01b1\3"+
		"\2\2\2\u01b1\u01bc\5d\63\2\u01b2\u01b4\7\177\2\2\u01b3\u01b2\3\2\2\2\u01b3"+
		"\u01b4\3\2\2\2\u01b4\u01b5\3\2\2\2\u01b5\u01b7\7\4\2\2\u01b6\u01b8\7\177"+
		"\2\2\u01b7\u01b6\3\2\2\2\u01b7\u01b8\3\2\2\2\u01b8\u01b9\3\2\2\2\u01b9"+
		"\u01bb\5d\63\2\u01ba\u01b3\3\2\2\2\u01bb\u01be\3\2\2\2\u01bc\u01ba\3\2"+
		"\2\2\u01bc\u01bd\3\2\2\2\u01bd%\3\2\2\2\u01be\u01bc\3\2\2\2\u01bf\u01c0"+
		"\7<\2\2\u01c0\u01c1\7\177\2\2\u01c1\u01cc\5(\25\2\u01c2\u01c4\7\177\2"+
		"\2\u01c3\u01c2\3\2\2\2\u01c3\u01c4\3\2\2\2\u01c4\u01c5\3\2\2\2\u01c5\u01c7"+
		"\7\4\2\2\u01c6\u01c8\7\177\2\2\u01c7\u01c6\3\2\2\2\u01c7\u01c8\3\2\2\2"+
		"\u01c8\u01c9\3\2\2\2\u01c9\u01cb\5(\25\2\u01ca\u01c3\3\2\2\2\u01cb\u01ce"+
		"\3\2\2\2\u01cc\u01ca\3\2\2\2\u01cc\u01cd\3\2\2\2\u01cd\'\3\2\2\2\u01ce"+
		"\u01cc\3\2\2\2\u01cf\u01d0\5\u00a6T\2\u01d0\u01d1\5Z.\2\u01d1\u01d4\3"+
		"\2\2\2\u01d2\u01d4\5\u00aeX\2\u01d3\u01cf\3\2\2\2\u01d3\u01d2\3\2\2\2"+
		"\u01d4)\3\2\2\2\u01d5\u01d6\7=\2\2\u01d6\u01d7\7\177\2\2\u01d7\u01de\5"+
		"\u0092J\2\u01d8\u01da\7\177\2\2\u01d9\u01d8\3\2\2\2\u01d9\u01da\3\2\2"+
		"\2\u01da\u01db\3\2\2\2\u01db\u01dc\7>\2\2\u01dc\u01dd\7\177\2\2\u01dd"+
		"\u01df\5.\30\2\u01de\u01d9\3\2\2\2\u01de\u01df\3\2\2\2\u01df+\3\2\2\2"+
		"\u01e0\u01e1\7=\2\2\u01e1\u01e4\7\177\2\2\u01e2\u01e5\5\u0092J\2\u01e3"+
		"\u01e5\5\u0094K\2\u01e4\u01e2\3\2\2\2\u01e4\u01e3\3\2\2\2\u01e5\u01ea"+
		"\3\2\2\2\u01e6\u01e7\7\177\2\2\u01e7\u01e8\7>\2\2\u01e8\u01e9\7\177\2"+
		"\2\u01e9\u01eb\5.\30\2\u01ea\u01e6\3\2\2\2\u01ea\u01eb\3\2\2\2\u01eb-"+
		"\3\2\2\2\u01ec\u01f7\5\60\31\2\u01ed\u01ef\7\177\2\2\u01ee\u01ed\3\2\2"+
		"\2\u01ee\u01ef\3\2\2\2\u01ef\u01f0\3\2\2\2\u01f0\u01f2\7\4\2\2\u01f1\u01f3"+
		"\7\177\2\2\u01f2\u01f1\3\2\2\2\u01f2\u01f3\3\2\2\2\u01f3\u01f4\3\2\2\2"+
		"\u01f4\u01f6\5\60\31\2\u01f5\u01ee\3\2\2\2\u01f6\u01f9\3\2\2\2\u01f7\u01f5"+
		"\3\2\2\2\u01f7\u01f8\3\2\2\2\u01f8\u01fc\3\2\2\2\u01f9\u01f7\3\2\2\2\u01fa"+
		"\u01fc\7\7\2\2\u01fb\u01ec\3\2\2\2\u01fb\u01fa\3\2\2\2\u01fc/\3\2\2\2"+
		"\u01fd\u01fe\5\u0096L\2\u01fe\u01ff\7\177\2\2\u01ff\u0200\7\65\2\2\u0200"+
		"\u0201\7\177\2\2\u0201\u0203\3\2\2\2\u0202\u01fd\3\2\2\2\u0202\u0203\3"+
		"\2\2\2\u0203\u0204\3\2\2\2\u0204\u0205\5\u00a6T\2\u0205\61\3\2\2\2\u0206"+
		"\u020b\7?\2\2\u0207\u0209\7\177\2\2\u0208\u0207\3\2\2\2\u0208\u0209\3"+
		"\2\2\2\u0209\u020a\3\2\2\2\u020a\u020c\7@\2\2\u020b\u0208\3\2\2\2\u020b"+
		"\u020c\3\2\2\2\u020c\u020d\3\2\2\2\u020d\u020e\7\177\2\2\u020e\u0213\5"+
		"\66\34\2\u020f\u0211\7\177\2\2\u0210\u020f\3\2\2\2\u0210\u0211\3\2\2\2"+
		"\u0211\u0212\3\2\2\2\u0212\u0214\5D#\2\u0213\u0210\3\2\2\2\u0213\u0214"+
		"\3\2\2\2\u0214\63\3\2\2\2\u0215\u021a\7A\2\2\u0216\u0218\7\177\2\2\u0217"+
		"\u0216\3\2\2\2\u0217\u0218\3\2\2\2\u0218\u0219\3\2\2\2\u0219\u021b\7@"+
		"\2\2\u021a\u0217\3\2\2\2\u021a\u021b\3\2\2\2\u021b\u021c\3\2\2\2\u021c"+
		"\u021d\7\177\2\2\u021d\u021e\5\66\34\2\u021e\65\3\2\2\2\u021f\u0222\5"+
		"8\35\2\u0220\u0221\7\177\2\2\u0221\u0223\5<\37\2\u0222\u0220\3\2\2\2\u0222"+
		"\u0223\3\2\2\2\u0223\u0226\3\2\2\2\u0224\u0225\7\177\2\2\u0225\u0227\5"+
		"> \2\u0226\u0224\3\2\2\2\u0226\u0227\3\2\2\2\u0227\u022a\3\2\2\2\u0228"+
		"\u0229\7\177\2\2\u0229\u022b\5@!\2\u022a\u0228\3\2\2\2\u022a\u022b\3\2"+
		"\2\2\u022b\67\3\2\2\2\u022c\u0237\7\b\2\2\u022d\u022f\7\177\2\2\u022e"+
		"\u022d\3\2\2\2\u022e\u022f\3\2\2\2\u022f\u0230\3\2\2\2\u0230\u0232\7\4"+
		"\2\2\u0231\u0233\7\177\2\2\u0232\u0231\3\2\2\2\u0232\u0233\3\2\2\2\u0233"+
		"\u0234\3\2\2\2\u0234\u0236\5:\36\2\u0235\u022e\3\2\2\2\u0236\u0239\3\2"+
		"\2\2\u0237\u0235\3\2\2\2\u0237\u0238\3\2\2\2\u0238\u0249\3\2\2\2\u0239"+
		"\u0237\3\2\2\2\u023a\u0245\5:\36\2\u023b\u023d\7\177\2\2\u023c\u023b\3"+
		"\2\2\2\u023c\u023d\3\2\2\2\u023d\u023e\3\2\2\2\u023e\u0240\7\4\2\2\u023f"+
		"\u0241\7\177\2\2\u0240\u023f\3\2\2\2\u0240\u0241\3\2\2\2\u0241\u0242\3"+
		"\2\2\2\u0242\u0244\5:\36\2\u0243\u023c\3\2\2\2\u0244\u0247\3\2\2\2\u0245"+
		"\u0243\3\2\2\2\u0245\u0246\3\2\2\2\u0246\u0249\3\2\2\2\u0247\u0245\3\2"+
		"\2\2\u0248\u022c\3\2\2\2\u0248\u023a\3\2\2\2\u02499\3\2\2\2\u024a\u024b"+
		"\5d\63\2\u024b\u024c\7\177\2\2\u024c\u024d\7\65\2\2\u024d\u024e\7\177"+
		"\2\2\u024e\u024f\5\u00a6T\2\u024f\u0252\3\2\2\2\u0250\u0252\5d\63\2\u0251"+
		"\u024a\3\2\2\2\u0251\u0250\3\2\2\2\u0252;\3\2\2\2\u0253\u0254\7B\2\2\u0254"+
		"\u0255\7\177\2\2\u0255\u0256\7C\2\2\u0256\u0257\7\177\2\2\u0257\u025f"+
		"\5B\"\2\u0258\u025a\7\4\2\2\u0259\u025b\7\177\2\2\u025a\u0259\3\2\2\2"+
		"\u025a\u025b\3\2\2\2\u025b\u025c\3\2\2\2\u025c\u025e\5B\"\2\u025d\u0258"+
		"\3\2\2\2\u025e\u0261\3\2\2\2\u025f\u025d\3\2\2\2\u025f\u0260\3\2\2\2\u0260"+
		"=\3\2\2\2\u0261\u025f\3\2\2\2\u0262\u0263\7D\2\2\u0263\u0264\7\177\2\2"+
		"\u0264\u0265\5d\63\2\u0265?\3\2\2\2\u0266\u0267\7E\2\2\u0267\u0268\7\177"+
		"\2\2\u0268\u0269\5d\63\2\u0269A\3\2\2\2\u026a\u026f\5d\63\2\u026b\u026d"+
		"\7\177\2\2\u026c\u026b\3\2\2\2\u026c\u026d\3\2\2\2\u026d\u026e\3\2\2\2"+
		"\u026e\u0270\t\2\2\2\u026f\u026c\3\2\2\2\u026f\u0270\3\2\2\2\u0270C\3"+
		"\2\2\2\u0271\u0272\7J\2\2\u0272\u0273\7\177\2\2\u0273\u0274\5d\63\2\u0274"+
		"E\3\2\2\2\u0275\u0280\5H%\2\u0276\u0278\7\177\2\2\u0277\u0276\3\2\2\2"+
		"\u0277\u0278\3\2\2\2\u0278\u0279\3\2\2\2\u0279\u027b\7\4\2\2\u027a\u027c"+
		"\7\177\2\2\u027b\u027a\3\2\2\2\u027b\u027c\3\2\2\2\u027c\u027d\3\2\2\2"+
		"\u027d\u027f\5H%\2\u027e\u0277\3\2\2\2\u027f\u0282\3\2\2\2\u0280\u027e"+
		"\3\2\2\2\u0280\u0281\3\2\2\2\u0281G\3\2\2\2\u0282\u0280\3\2\2\2\u0283"+
		"\u0285\5\u00a6T\2\u0284\u0286\7\177\2\2\u0285\u0284\3\2\2\2\u0285\u0286"+
		"\3\2\2\2\u0286\u0287\3\2\2\2\u0287\u0289\7\5\2\2\u0288\u028a\7\177\2\2"+
		"\u0289\u0288\3\2\2\2\u0289\u028a\3\2\2\2\u028a\u028b\3\2\2\2\u028b\u028c"+
		"\5J&\2\u028c\u028f\3\2\2\2\u028d\u028f\5J&\2\u028e\u0283\3\2\2\2\u028e"+
		"\u028d\3\2\2\2\u028fI\3\2\2\2\u0290\u0291\5L\'\2\u0291K\3\2\2\2\u0292"+
		"\u0299\5N(\2\u0293\u0295\7\177\2\2\u0294\u0293\3\2\2\2\u0294\u0295\3\2"+
		"\2\2\u0295\u0296\3\2\2\2\u0296\u0298\5P)\2\u0297\u0294\3\2\2\2\u0298\u029b"+
		"\3\2\2\2\u0299\u0297\3\2\2\2\u0299\u029a\3\2\2\2\u029a\u02a1\3\2\2\2\u029b"+
		"\u0299\3\2\2\2\u029c\u029d\7\t\2\2\u029d\u029e\5L\'\2\u029e\u029f\7\n"+
		"\2\2\u029f\u02a1\3\2\2\2\u02a0\u0292\3\2\2\2\u02a0\u029c\3\2\2\2\u02a1"+
		"M\3\2\2\2\u02a2\u02a4\7\t\2\2\u02a3\u02a5\7\177\2\2\u02a4\u02a3\3\2\2"+
		"\2\u02a4\u02a5\3\2\2\2\u02a5\u02aa\3\2\2\2\u02a6\u02a8\5\u00a6T\2\u02a7"+
		"\u02a9\7\177\2\2\u02a8\u02a7\3\2\2\2\u02a8\u02a9\3\2\2\2\u02a9\u02ab\3"+
		"\2\2\2\u02aa\u02a6\3\2\2\2\u02aa\u02ab\3\2\2\2\u02ab\u02b0\3\2\2\2\u02ac"+
		"\u02ae\5Z.\2\u02ad\u02af\7\177\2\2\u02ae\u02ad\3\2\2\2\u02ae\u02af\3\2"+
		"\2\2\u02af\u02b1\3\2\2\2\u02b0\u02ac\3\2\2\2\u02b0\u02b1\3\2\2\2\u02b1"+
		"\u02b6\3\2\2\2\u02b2\u02b4\5V,\2\u02b3\u02b5\7\177\2\2\u02b4\u02b3\3\2"+
		"\2\2\u02b4\u02b5\3\2\2\2\u02b5\u02b7\3\2\2\2\u02b6\u02b2\3\2\2\2\u02b6"+
		"\u02b7\3\2\2\2\u02b7\u02b8\3\2\2\2\u02b8\u02b9\7\n\2\2\u02b9O\3\2\2\2"+
		"\u02ba\u02bc\5R*\2\u02bb\u02bd\7\177\2\2\u02bc\u02bb\3\2\2\2\u02bc\u02bd"+
		"\3\2\2\2\u02bd\u02be\3\2\2\2\u02be\u02bf\5N(\2\u02bfQ\3\2\2\2\u02c0\u02c2"+
		"\5\u00bc_\2\u02c1\u02c3\7\177\2\2\u02c2\u02c1\3\2\2\2\u02c2\u02c3\3\2"+
		"\2\2\u02c3\u02c4\3\2\2\2\u02c4\u02c6\5\u00c0a\2\u02c5\u02c7\7\177\2\2"+
		"\u02c6\u02c5\3\2\2\2\u02c6\u02c7\3\2\2\2\u02c7\u02c9\3\2\2\2\u02c8\u02ca"+
		"\5T+\2\u02c9\u02c8\3\2\2\2\u02c9\u02ca\3\2\2\2\u02ca\u02cc\3\2\2\2\u02cb"+
		"\u02cd\7\177\2\2\u02cc\u02cb\3\2\2\2\u02cc\u02cd\3\2\2\2\u02cd\u02ce\3"+
		"\2\2\2\u02ce\u02d0\5\u00c0a\2\u02cf\u02d1\7\177\2\2\u02d0\u02cf\3\2\2"+
		"\2\u02d0\u02d1\3\2\2\2\u02d1\u02d2\3\2\2\2\u02d2\u02d3\5\u00be`\2\u02d3"+
		"\u0301\3\2\2\2\u02d4\u02d6\5\u00bc_\2\u02d5\u02d7\7\177\2\2\u02d6\u02d5"+
		"\3\2\2\2\u02d6\u02d7\3\2\2\2\u02d7\u02d8\3\2\2\2\u02d8\u02da\5\u00c0a"+
		"\2\u02d9\u02db\7\177\2\2\u02da\u02d9\3\2\2\2\u02da\u02db\3\2\2\2\u02db"+
		"\u02dd\3\2\2\2\u02dc\u02de\5T+\2\u02dd\u02dc\3\2\2\2\u02dd\u02de\3\2\2"+
		"\2\u02de\u02e0\3\2\2\2\u02df\u02e1\7\177\2\2\u02e0\u02df\3\2\2\2\u02e0"+
		"\u02e1\3\2\2\2\u02e1\u02e2\3\2\2\2\u02e2\u02e3\5\u00c0a\2\u02e3\u0301"+
		"\3\2\2\2\u02e4\u02e6\5\u00c0a\2\u02e5\u02e7\7\177\2\2\u02e6\u02e5\3\2"+
		"\2\2\u02e6\u02e7\3\2\2\2\u02e7\u02e9\3\2\2\2\u02e8\u02ea\5T+\2\u02e9\u02e8"+
		"\3\2\2\2\u02e9\u02ea\3\2\2\2\u02ea\u02ec\3\2\2\2\u02eb\u02ed\7\177\2\2"+
		"\u02ec\u02eb\3\2\2\2\u02ec\u02ed\3\2\2\2\u02ed\u02ee\3\2\2\2\u02ee\u02f0"+
		"\5\u00c0a\2\u02ef\u02f1\7\177\2\2\u02f0\u02ef\3\2\2\2\u02f0\u02f1\3\2"+
		"\2\2\u02f1\u02f2\3\2\2\2\u02f2\u02f3\5\u00be`\2\u02f3\u0301\3\2\2\2\u02f4"+
		"\u02f6\5\u00c0a\2\u02f5\u02f7\7\177\2\2\u02f6\u02f5\3\2\2\2\u02f6\u02f7"+
		"\3\2\2\2\u02f7\u02f9\3\2\2\2\u02f8\u02fa\5T+\2\u02f9\u02f8\3\2\2\2\u02f9"+
		"\u02fa\3\2\2\2\u02fa\u02fc\3\2\2\2\u02fb\u02fd\7\177\2\2\u02fc\u02fb\3"+
		"\2\2\2\u02fc\u02fd\3\2\2\2\u02fd\u02fe\3\2\2\2\u02fe\u02ff\5\u00c0a\2"+
		"\u02ff\u0301\3\2\2\2\u0300\u02c0\3\2\2\2\u0300\u02d4\3\2\2\2\u0300\u02e4"+
		"\3\2\2\2\u0300\u02f4\3\2\2\2\u0301S\3\2\2\2\u0302\u0304\7\13\2\2\u0303"+
		"\u0305\7\177\2\2\u0304\u0303\3\2\2\2\u0304\u0305\3\2\2\2\u0305\u030a\3"+
		"\2\2\2\u0306\u0308\5\u00a6T\2\u0307\u0309\7\177\2\2\u0308\u0307\3\2\2"+
		"\2\u0308\u0309\3\2\2\2\u0309\u030b\3\2\2\2\u030a\u0306\3\2\2\2\u030a\u030b"+
		"\3\2\2\2\u030b\u0310\3\2\2\2\u030c\u030e\5X-\2\u030d\u030f\7\177\2\2\u030e"+
		"\u030d\3\2\2\2\u030e\u030f\3\2\2\2\u030f\u0311\3\2\2\2\u0310\u030c\3\2"+
		"\2\2\u0310\u0311\3\2\2\2\u0311\u0313\3\2\2\2\u0312\u0314\5^\60\2\u0313"+
		"\u0312\3\2\2\2\u0313\u0314\3\2\2\2\u0314\u0319\3\2\2\2\u0315\u0317\5V"+
		",\2\u0316\u0318\7\177\2\2\u0317\u0316\3\2\2\2\u0317\u0318\3\2\2\2\u0318"+
		"\u031a\3\2\2\2\u0319\u0315\3\2\2\2\u0319\u031a\3\2\2\2\u031a\u031b\3\2"+
		"\2\2\u031b\u031c\7\f\2\2\u031cU\3\2\2\2\u031d\u0320\5\u00aaV\2\u031e\u0320"+
		"\5\u00acW\2\u031f\u031d\3\2\2\2\u031f\u031e\3\2\2\2\u0320W\3\2\2\2\u0321"+
		"\u0323\7\r\2\2\u0322\u0324\7\177\2\2\u0323\u0322\3\2\2\2\u0323\u0324\3"+
		"\2\2\2\u0324\u0325\3\2\2\2\u0325\u0333\5b\62\2\u0326\u0328\7\177\2\2\u0327"+
		"\u0326\3\2\2\2\u0327\u0328\3\2\2\2\u0328\u0329\3\2\2\2\u0329\u032b\7\16"+
		"\2\2\u032a\u032c\7\r\2\2\u032b\u032a\3\2\2\2\u032b\u032c\3\2\2\2\u032c"+
		"\u032e\3\2\2\2\u032d\u032f\7\177\2\2\u032e\u032d\3\2\2\2\u032e\u032f\3"+
		"\2\2\2\u032f\u0330\3\2\2\2\u0330\u0332\5b\62\2\u0331\u0327\3\2\2\2\u0332"+
		"\u0335\3\2\2\2\u0333\u0331\3\2\2\2\u0333\u0334\3\2\2\2\u0334Y\3\2\2\2"+
		"\u0335\u0333\3\2\2\2\u0336\u033d\5\\/\2\u0337\u0339\7\177\2\2\u0338\u0337"+
		"\3\2\2\2\u0338\u0339\3\2\2\2\u0339\u033a\3\2\2\2\u033a\u033c\5\\/\2\u033b"+
		"\u0338\3\2\2\2\u033c\u033f\3\2\2\2\u033d\u033b\3\2\2\2\u033d\u033e\3\2"+
		"\2\2\u033e[\3\2\2\2\u033f\u033d\3\2\2\2\u0340\u0342\7\r\2\2\u0341\u0343"+
		"\7\177\2\2\u0342\u0341\3\2\2\2\u0342\u0343\3\2\2\2\u0343\u0344\3\2\2\2"+
		"\u0344\u0345\5`\61\2\u0345]\3\2\2\2\u0346\u0348\7\b\2\2\u0347\u0349\7"+
		"\177\2\2\u0348\u0347\3\2\2\2\u0348\u0349\3\2\2\2\u0349\u034e\3\2\2\2\u034a"+
		"\u034c\5\u00b2Z\2\u034b\u034d\7\177\2\2\u034c\u034b\3\2\2\2\u034c\u034d"+
		"\3\2\2\2\u034d\u034f\3\2\2\2\u034e\u034a\3\2\2\2\u034e\u034f\3\2\2\2\u034f"+
		"\u035a\3\2\2\2\u0350\u0352\7\17\2\2\u0351\u0353\7\177\2\2\u0352\u0351"+
		"\3\2\2\2\u0352\u0353\3\2\2\2\u0353\u0358\3\2\2\2\u0354\u0356\5\u00b2Z"+
		"\2\u0355\u0357\7\177\2\2\u0356\u0355\3\2\2\2\u0356\u0357\3\2\2\2\u0357"+
		"\u0359\3\2\2\2\u0358\u0354\3\2\2\2\u0358\u0359\3\2\2\2\u0359\u035b\3\2"+
		"\2\2\u035a\u0350\3\2\2\2\u035a\u035b\3\2\2\2\u035b_\3\2\2\2\u035c\u035d"+
		"\5\u00b6\\\2\u035da\3\2\2\2\u035e\u035f\5\u00b6\\\2\u035fc\3\2\2\2\u0360"+
		"\u0361\5f\64\2\u0361e\3\2\2\2\u0362\u0369\5h\65\2\u0363\u0364\7\177\2"+
		"\2\u0364\u0365\7K\2\2\u0365\u0366\7\177\2\2\u0366\u0368\5h\65\2\u0367"+
		"\u0363\3\2\2\2\u0368\u036b\3\2\2\2\u0369\u0367\3\2\2\2\u0369\u036a\3\2"+
		"\2\2\u036ag\3\2\2\2\u036b\u0369\3\2\2\2\u036c\u0373\5j\66\2\u036d\u036e"+
		"\7\177\2\2\u036e\u036f\7L\2\2\u036f\u0370\7\177\2\2\u0370\u0372\5j\66"+
		"\2\u0371\u036d\3\2\2\2\u0372\u0375\3\2\2\2\u0373\u0371\3\2\2\2\u0373\u0374"+
		"\3\2\2\2\u0374i\3\2\2\2\u0375\u0373\3\2\2\2\u0376\u037d\5l\67\2\u0377"+
		"\u0378\7\177\2\2\u0378\u0379\7M\2\2\u0379\u037a\7\177\2\2\u037a\u037c"+
		"\5l\67\2\u037b\u0377\3\2\2\2\u037c\u037f\3\2\2\2\u037d\u037b\3\2\2\2\u037d"+
		"\u037e\3\2\2\2\u037ek\3\2\2\2\u037f\u037d\3\2\2\2\u0380\u0382\7N\2\2\u0381"+
		"\u0383\7\177\2\2\u0382\u0381\3\2\2\2\u0382\u0383\3\2\2\2\u0383\u0385\3"+
		"\2\2\2\u0384\u0380\3\2\2\2\u0385\u0388\3\2\2\2\u0386\u0384\3\2\2\2\u0386"+
		"\u0387\3\2\2\2\u0387\u0389\3\2\2\2\u0388\u0386\3\2\2\2\u0389\u038a\5n"+
		"8\2\u038am\3\2\2\2\u038b\u0392\5p9\2\u038c\u038e\7\177\2\2\u038d\u038c"+
		"\3\2\2\2\u038d\u038e\3\2\2\2\u038e\u038f\3\2\2\2\u038f\u0391\5\u0084C"+
		"\2\u0390\u038d\3\2\2\2\u0391\u0394\3\2\2\2\u0392\u0390\3\2\2\2\u0392\u0393"+
		"\3\2\2\2\u0393o\3\2\2\2\u0394\u0392\3\2\2\2\u0395\u03a8\5r:\2\u0396\u0398"+
		"\7\177\2\2\u0397\u0396\3\2\2\2\u0397\u0398\3\2\2\2\u0398\u0399\3\2\2\2"+
		"\u0399\u039b\7\20\2\2\u039a\u039c\7\177\2\2\u039b\u039a\3\2\2\2\u039b"+
		"\u039c\3\2\2\2\u039c\u039d\3\2\2\2\u039d\u03a7\5r:\2\u039e\u03a0\7\177"+
		"\2\2\u039f\u039e\3\2\2\2\u039f\u03a0\3\2\2\2\u03a0\u03a1\3\2\2\2\u03a1"+
		"\u03a3\7\7\2\2\u03a2\u03a4\7\177\2\2\u03a3\u03a2\3\2\2\2\u03a3\u03a4\3"+
		"\2\2\2\u03a4\u03a5\3\2\2\2\u03a5\u03a7\5r:\2\u03a6\u0397\3\2\2\2\u03a6"+
		"\u039f\3\2\2\2\u03a7\u03aa\3\2\2\2\u03a8\u03a6\3\2\2\2\u03a8\u03a9\3\2"+
		"\2\2\u03a9q\3\2\2\2\u03aa\u03a8\3\2\2\2\u03ab\u03c6\5t;\2\u03ac\u03ae"+
		"\7\177\2\2\u03ad\u03ac\3\2\2\2\u03ad\u03ae\3\2\2\2\u03ae\u03af\3\2\2\2"+
		"\u03af\u03b1\7\b\2\2\u03b0\u03b2\7\177\2\2\u03b1\u03b0\3\2\2\2\u03b1\u03b2"+
		"\3\2\2\2\u03b2\u03b3\3\2\2\2\u03b3\u03c5\5t;\2\u03b4\u03b6\7\177\2\2\u03b5"+
		"\u03b4\3\2\2\2\u03b5\u03b6\3\2\2\2\u03b6\u03b7\3\2\2\2\u03b7\u03b9\7\21"+
		"\2\2\u03b8\u03ba\7\177\2\2\u03b9\u03b8\3\2\2\2\u03b9\u03ba\3\2\2\2\u03ba"+
		"\u03bb\3\2\2\2\u03bb\u03c5\5t;\2\u03bc\u03be\7\177\2\2\u03bd\u03bc\3\2"+
		"\2\2\u03bd\u03be\3\2\2\2\u03be\u03bf\3\2\2\2\u03bf\u03c1\7\22\2\2\u03c0"+
		"\u03c2\7\177\2\2\u03c1\u03c0\3\2\2\2\u03c1\u03c2\3\2\2\2\u03c2\u03c3\3"+
		"\2\2\2\u03c3\u03c5\5t;\2\u03c4\u03ad\3\2\2\2\u03c4\u03b5\3\2\2\2\u03c4"+
		"\u03bd\3\2\2\2\u03c5\u03c8\3\2\2\2\u03c6\u03c4\3\2\2\2\u03c6\u03c7\3\2"+
		"\2\2\u03c7s\3\2\2\2\u03c8\u03c6\3\2\2\2\u03c9\u03d4\5v<\2\u03ca\u03cc"+
		"\7\177\2\2\u03cb\u03ca\3\2\2\2\u03cb\u03cc\3\2\2\2\u03cc\u03cd\3\2\2\2"+
		"\u03cd\u03cf\7\23\2\2\u03ce\u03d0\7\177\2\2\u03cf\u03ce\3\2\2\2\u03cf"+
		"\u03d0\3\2\2\2\u03d0\u03d1\3\2\2\2\u03d1\u03d3\5v<\2\u03d2\u03cb\3\2\2"+
		"\2\u03d3\u03d6\3\2\2\2\u03d4\u03d2\3\2\2\2\u03d4\u03d5\3\2\2\2\u03d5u"+
		"\3\2\2\2\u03d6\u03d4\3\2\2\2\u03d7\u03d9\t\3\2\2\u03d8\u03da\7\177\2\2"+
		"\u03d9\u03d8\3\2\2\2\u03d9\u03da\3\2\2\2\u03da\u03dc\3\2\2\2\u03db\u03d7"+
		"\3\2\2\2\u03dc\u03df\3\2\2\2\u03dd\u03db\3\2\2\2\u03dd\u03de\3\2\2\2\u03de"+
		"\u03e0\3\2\2\2\u03df\u03dd\3\2\2\2\u03e0\u03e1\5x=\2\u03e1w\3\2\2\2\u03e2"+
		"\u0414\5z>\2\u03e3\u03e5\7\177\2\2\u03e4\u03e3\3\2\2\2\u03e4\u03e5\3\2"+
		"\2\2\u03e5\u03e6\3\2\2\2\u03e6\u03e7\7\13\2\2\u03e7\u03e8\5d\63\2\u03e8"+
		"\u03e9\7\f\2\2\u03e9\u0413\3\2\2\2\u03ea\u03ec\7\177\2\2\u03eb\u03ea\3"+
		"\2\2\2\u03eb\u03ec\3\2\2\2\u03ec\u03ed\3\2\2\2\u03ed\u03ef\7\13\2\2\u03ee"+
		"\u03f0\5d\63\2\u03ef\u03ee\3\2\2\2\u03ef\u03f0\3\2\2\2\u03f0\u03f1\3\2"+
		"\2\2\u03f1\u03f3\7\17\2\2\u03f2\u03f4\5d\63\2\u03f3\u03f2\3\2\2\2\u03f3"+
		"\u03f4\3\2\2\2\u03f4\u03f5\3\2\2\2\u03f5\u0413\7\f\2\2\u03f6\u03f7\7\177"+
		"\2\2\u03f7\u0403\7O\2\2\u03f8\u03f9\7\177\2\2\u03f9\u03fa\7P\2\2\u03fa"+
		"\u03fb\7\177\2\2\u03fb\u0403\7?\2\2\u03fc\u03fd\7\177\2\2\u03fd\u03fe"+
		"\7Q\2\2\u03fe\u03ff\7\177\2\2\u03ff\u0403\7?\2\2\u0400\u0401\7\177\2\2"+
		"\u0401\u0403\7R\2\2\u0402\u03f6\3\2\2\2\u0402\u03f8\3\2\2\2\u0402\u03fc"+
		"\3\2\2\2\u0402\u0400\3\2\2\2\u0403\u0405\3\2\2\2\u0404\u0406\7\177\2\2"+
		"\u0405\u0404\3\2\2\2\u0405\u0406\3\2\2\2\u0406\u0407\3\2\2\2\u0407\u0413"+
		"\5z>\2\u0408\u0409\7\177\2\2\u0409\u040a\7S\2\2\u040a\u040b\7\177\2\2"+
		"\u040b\u0413\7T\2\2\u040c\u040d\7\177\2\2\u040d\u040e\7S\2\2\u040e\u040f"+
		"\7\177\2\2\u040f\u0410\7N\2\2\u0410\u0411\7\177\2\2\u0411\u0413\7T\2\2"+
		"\u0412\u03e4\3\2\2\2\u0412\u03eb\3\2\2\2\u0412\u0402\3\2\2\2\u0412\u0408"+
		"\3\2\2\2\u0412\u040c\3\2\2\2\u0413\u0416\3\2\2\2\u0414\u0412\3\2\2\2\u0414"+
		"\u0415\3\2\2\2\u0415y\3\2\2\2\u0416\u0414\3\2\2\2\u0417\u041e\5|?\2\u0418"+
		"\u041a\7\177\2\2\u0419\u0418\3\2\2\2\u0419\u041a\3\2\2\2\u041a\u041b\3"+
		"\2\2\2\u041b\u041d\5\u00a0Q\2\u041c\u0419\3\2\2\2\u041d\u0420\3\2\2\2"+
		"\u041e\u041c\3\2\2\2\u041e\u041f\3\2\2\2\u041f\u0425\3\2\2\2\u0420\u041e"+
		"\3\2\2\2\u0421\u0423\7\177\2\2\u0422\u0421\3\2\2\2\u0422\u0423\3\2\2\2"+
		"\u0423\u0424\3\2\2\2\u0424\u0426\5Z.\2\u0425\u0422\3\2\2\2\u0425\u0426"+
		"\3\2\2\2\u0426{\3\2\2\2\u0427\u0499\5~@\2\u0428\u0499\5\u00acW\2\u0429"+
		"\u0499\5\u00a2R\2\u042a\u042c\7U\2\2\u042b\u042d\7\177\2\2\u042c\u042b"+
		"\3\2\2\2\u042c\u042d\3\2\2\2\u042d\u042e\3\2\2\2\u042e\u0430\7\t\2\2\u042f"+
		"\u0431\7\177\2\2\u0430\u042f\3\2\2\2\u0430\u0431\3\2\2\2\u0431\u0432\3"+
		"\2\2\2\u0432\u0434\7\b\2\2\u0433\u0435\7\177\2\2\u0434\u0433\3\2\2\2\u0434"+
		"\u0435\3\2\2\2\u0435\u0436\3\2\2\2\u0436\u0499\7\n\2\2\u0437\u0499\5\u009c"+
		"O\2\u0438\u0499\5\u009eP\2\u0439\u043b\7V\2\2\u043a\u043c\7\177\2\2\u043b"+
		"\u043a\3\2\2\2\u043b\u043c\3\2\2\2\u043c\u043d\3\2\2\2\u043d\u043f\7\t"+
		"\2\2\u043e\u0440\7\177\2\2\u043f\u043e\3\2\2\2\u043f\u0440\3\2\2\2\u0440"+
		"\u0441\3\2\2\2\u0441\u0443\5\u008aF\2\u0442\u0444\7\177\2\2\u0443\u0442"+
		"\3\2\2\2\u0443\u0444\3\2\2\2\u0444\u0445\3\2\2\2\u0445\u0446\7\n\2\2\u0446"+
		"\u0499\3\2\2\2\u0447\u0449\7W\2\2\u0448\u044a\7\177\2\2\u0449\u0448\3"+
		"\2\2\2\u0449\u044a\3\2\2\2\u044a\u044b\3\2\2\2\u044b\u044d\7\t\2\2\u044c"+
		"\u044e\7\177\2\2\u044d\u044c\3\2\2\2\u044d\u044e\3\2\2\2\u044e\u044f\3"+
		"\2\2\2\u044f\u0451\5\u008aF\2\u0450\u0452\7\177\2\2\u0451\u0450\3\2\2"+
		"\2\u0451\u0452\3\2\2\2\u0452\u0458\3\2\2\2\u0453\u0455\7\177\2\2\u0454"+
		"\u0453\3\2\2\2\u0454\u0455\3\2\2\2\u0455\u0456\3\2\2\2\u0456\u0457\7\16"+
		"\2\2\u0457\u0459\5d\63\2\u0458\u0454\3\2\2\2\u0458\u0459\3\2\2\2\u0459"+
		"\u045a\3\2\2\2\u045a\u045b\7\n\2\2\u045b\u0499\3\2\2\2\u045c\u045e\7\61"+
		"\2\2\u045d\u045f\7\177\2\2\u045e\u045d\3\2\2\2\u045e\u045f\3\2\2\2\u045f"+
		"\u0460\3\2\2\2\u0460\u0462\7\t\2\2\u0461\u0463\7\177\2\2\u0462\u0461\3"+
		"\2\2\2\u0462\u0463\3\2\2\2\u0463\u0464\3\2\2\2\u0464\u0466\5\u008aF\2"+
		"\u0465\u0467\7\177\2\2\u0466\u0465\3\2\2\2\u0466\u0467\3\2\2\2\u0467\u0468"+
		"\3\2\2\2\u0468\u0469\7\n\2\2\u0469\u0499\3\2\2\2\u046a\u046c\7X\2\2\u046b"+
		"\u046d\7\177\2\2\u046c\u046b\3\2\2\2\u046c\u046d\3\2\2\2\u046d\u046e\3"+
		"\2\2\2\u046e\u0470\7\t\2\2\u046f\u0471\7\177\2\2\u0470\u046f\3\2\2\2\u0470"+
		"\u0471\3\2\2\2\u0471\u0472\3\2\2\2\u0472\u0474\5\u008aF\2\u0473\u0475"+
		"\7\177\2\2\u0474\u0473\3\2\2\2\u0474\u0475\3\2\2\2\u0475\u0476\3\2\2\2"+
		"\u0476\u0477\7\n\2\2\u0477\u0499\3\2\2\2\u0478\u047a\7Y\2\2\u0479\u047b"+
		"\7\177\2\2\u047a\u0479\3\2\2\2\u047a\u047b\3\2\2\2\u047b\u047c\3\2\2\2"+
		"\u047c\u047e\7\t\2\2\u047d\u047f\7\177\2\2\u047e\u047d\3\2\2\2\u047e\u047f"+
		"\3\2\2\2\u047f\u0480\3\2\2\2\u0480\u0482\5\u008aF\2\u0481\u0483\7\177"+
		"\2\2\u0482\u0481\3\2\2\2\u0482\u0483\3\2\2\2\u0483\u0484\3\2\2\2\u0484"+
		"\u0485\7\n\2\2\u0485\u0499\3\2\2\2\u0486\u0488\7Z\2\2\u0487\u0489\7\177"+
		"\2\2\u0488\u0487\3\2\2\2\u0488\u0489\3\2\2\2\u0489\u048a\3\2\2\2\u048a"+
		"\u048c\7\t\2\2\u048b\u048d\7\177\2\2\u048c\u048b\3\2\2\2\u048c\u048d\3"+
		"\2\2\2\u048d\u048e\3\2\2\2\u048e\u0490\5\u008aF\2\u048f\u0491\7\177\2"+
		"\2\u0490\u048f\3\2\2\2\u0490\u0491\3\2\2\2\u0491\u0492\3\2\2\2\u0492\u0493"+
		"\7\n\2\2\u0493\u0499\3\2\2\2\u0494\u0499\5\u0088E\2\u0495\u0499\5\u0086"+
		"D\2\u0496\u0499\5\u008eH\2\u0497\u0499\5\u00a6T\2\u0498\u0427\3\2\2\2"+
		"\u0498\u0428\3\2\2\2\u0498\u0429\3\2\2\2\u0498\u042a\3\2\2\2\u0498\u0437"+
		"\3\2\2\2\u0498\u0438\3\2\2\2\u0498\u0439\3\2\2\2\u0498\u0447\3\2\2\2\u0498"+
		"\u045c\3\2\2\2\u0498\u046a\3\2\2\2\u0498\u0478\3\2\2\2\u0498\u0486\3\2"+
		"\2\2\u0498\u0494\3\2\2\2\u0498\u0495\3\2\2\2\u0498\u0496\3\2\2\2\u0498"+
		"\u0497\3\2\2\2\u0499}\3\2\2\2\u049a\u04a1\5\u00a8U\2\u049b\u04a1\7c\2"+
		"\2\u049c\u04a1\5\u0080A\2\u049d\u04a1\7T\2\2\u049e\u04a1\5\u00aaV\2\u049f"+
		"\u04a1\5\u0082B\2\u04a0\u049a\3\2\2\2\u04a0\u049b\3\2\2\2\u04a0\u049c"+
		"\3\2\2\2\u04a0\u049d\3\2\2\2\u04a0\u049e\3\2\2\2\u04a0\u049f\3\2\2\2\u04a1"+
		"\177\3\2\2\2\u04a2\u04a3\t\4\2\2\u04a3\u0081\3\2\2\2\u04a4\u04a6\7\13"+
		"\2\2\u04a5\u04a7\7\177\2\2\u04a6\u04a5\3\2\2\2\u04a6\u04a7\3\2\2\2\u04a7"+
		"\u04b9\3\2\2\2\u04a8\u04aa\5d\63\2\u04a9\u04ab\7\177\2\2\u04aa\u04a9\3"+
		"\2\2\2\u04aa\u04ab\3\2\2\2\u04ab\u04b6\3\2\2\2\u04ac\u04ae\7\4\2\2\u04ad"+
		"\u04af\7\177\2\2\u04ae\u04ad\3\2\2\2\u04ae\u04af\3\2\2\2\u04af\u04b0\3"+
		"\2\2\2\u04b0\u04b2\5d\63\2\u04b1\u04b3\7\177\2\2\u04b2\u04b1\3\2\2\2\u04b2"+
		"\u04b3\3\2\2\2\u04b3\u04b5\3\2\2\2\u04b4\u04ac\3\2\2\2\u04b5\u04b8\3\2"+
		"\2\2\u04b6\u04b4\3\2\2\2\u04b6\u04b7\3\2\2\2\u04b7\u04ba\3\2\2\2\u04b8"+
		"\u04b6\3\2\2\2\u04b9\u04a8\3\2\2\2\u04b9\u04ba\3\2\2\2\u04ba\u04bb\3\2"+
		"\2\2\u04bb\u04bc\7\f\2\2\u04bc\u0083\3\2\2\2\u04bd\u04bf\7\5\2\2\u04be"+
		"\u04c0\7\177\2\2\u04bf\u04be\3\2\2\2\u04bf\u04c0\3\2\2\2\u04c0\u04c1\3"+
		"\2\2\2\u04c1\u04dc\5p9\2\u04c2\u04c4\7\24\2\2\u04c3\u04c5\7\177\2\2\u04c4"+
		"\u04c3\3\2\2\2\u04c4\u04c5\3\2\2\2\u04c5\u04c6\3\2\2\2\u04c6\u04dc\5p"+
		"9\2\u04c7\u04c9\7\25\2\2\u04c8\u04ca\7\177\2\2\u04c9\u04c8\3\2\2\2\u04c9"+
		"\u04ca\3\2\2\2\u04ca\u04cb\3\2\2\2\u04cb\u04dc\5p9\2\u04cc\u04ce\7\26"+
		"\2\2\u04cd\u04cf\7\177\2\2\u04ce\u04cd\3\2\2\2\u04ce\u04cf\3\2\2\2\u04cf"+
		"\u04d0\3\2\2\2\u04d0\u04dc\5p9\2\u04d1\u04d3\7\27\2\2\u04d2\u04d4\7\177"+
		"\2\2\u04d3\u04d2\3\2\2\2\u04d3\u04d4\3\2\2\2\u04d4\u04d5\3\2\2\2\u04d5"+
		"\u04dc\5p9\2\u04d6\u04d8\7\30\2\2\u04d7\u04d9\7\177\2\2\u04d8\u04d7\3"+
		"\2\2\2\u04d8\u04d9\3\2\2\2\u04d9\u04da\3\2\2\2\u04da\u04dc\5p9\2\u04db"+
		"\u04bd\3\2\2\2\u04db\u04c2\3\2\2\2\u04db\u04c7\3\2\2\2\u04db\u04cc\3\2"+
		"\2\2\u04db\u04d1\3\2\2\2\u04db\u04d6\3\2\2\2\u04dc\u0085\3\2\2\2\u04dd"+
		"\u04df\7\t\2\2\u04de\u04e0\7\177\2\2\u04df\u04de\3\2\2\2\u04df\u04e0\3"+
		"\2\2\2\u04e0\u04e1\3\2\2\2\u04e1\u04e3\5d\63\2\u04e2\u04e4\7\177\2\2\u04e3"+
		"\u04e2\3\2\2\2\u04e3\u04e4\3\2\2\2\u04e4\u04e5\3\2\2\2\u04e5\u04e6\7\n"+
		"\2\2\u04e6\u0087\3\2\2\2\u04e7\u04ec\5N(\2\u04e8\u04ea\7\177\2\2\u04e9"+
		"\u04e8\3\2\2\2\u04e9\u04ea\3\2\2\2\u04ea\u04eb\3\2\2\2\u04eb\u04ed\5P"+
		")\2\u04ec\u04e9\3\2\2\2\u04ed\u04ee\3\2\2\2\u04ee\u04ec\3\2\2\2\u04ee"+
		"\u04ef\3\2\2\2\u04ef\u0089\3\2\2\2\u04f0\u04f5\5\u008cG\2\u04f1\u04f3"+
		"\7\177\2\2\u04f2\u04f1\3\2\2\2\u04f2\u04f3\3\2\2\2\u04f3\u04f4\3\2\2\2"+
		"\u04f4\u04f6\5D#\2\u04f5\u04f2\3\2\2\2\u04f5\u04f6\3\2\2\2\u04f6\u008b"+
		"\3\2\2\2\u04f7\u04f8\5\u00a6T\2\u04f8\u04f9\7\177\2\2\u04f9\u04fa\7O\2"+
		"\2\u04fa\u04fb\7\177\2\2\u04fb\u04fc\5d\63\2\u04fc\u008d\3\2\2\2\u04fd"+
		"\u04ff\5\u0090I\2\u04fe\u0500\7\177\2\2\u04ff\u04fe\3\2\2\2\u04ff\u0500"+
		"\3\2\2\2\u0500\u0501\3\2\2\2\u0501\u0503\7\t\2\2\u0502\u0504\7\177\2\2"+
		"\u0503\u0502\3\2\2\2\u0503\u0504\3\2\2\2\u0504\u0509\3\2\2\2\u0505\u0507"+
		"\7@\2\2\u0506\u0508\7\177\2\2\u0507\u0506\3\2\2\2\u0507\u0508\3\2\2\2"+
		"\u0508\u050a\3\2\2\2\u0509\u0505\3\2\2\2\u0509\u050a\3\2\2\2\u050a\u051c"+
		"\3\2\2\2\u050b\u050d\5d\63\2\u050c\u050e\7\177\2\2\u050d\u050c\3\2\2\2"+
		"\u050d\u050e\3\2\2\2\u050e\u0519\3\2\2\2\u050f\u0511\7\4\2\2\u0510\u0512"+
		"\7\177\2\2\u0511\u0510\3\2\2\2\u0511\u0512\3\2\2\2\u0512\u0513\3\2\2\2"+
		"\u0513\u0515\5d\63\2\u0514\u0516\7\177\2\2\u0515\u0514\3\2\2\2\u0515\u0516"+
		"\3\2\2\2\u0516\u0518\3\2\2\2\u0517\u050f\3\2\2\2\u0518\u051b\3\2\2\2\u0519"+
		"\u0517\3\2\2\2\u0519\u051a\3\2\2\2\u051a\u051d\3\2\2\2\u051b\u0519\3\2"+
		"\2\2\u051c\u050b\3\2\2\2\u051c\u051d\3\2\2\2\u051d\u051e\3\2\2\2\u051e"+
		"\u051f\7\n\2\2\u051f\u008f\3\2\2\2\u0520\u0523\5\u00ba^\2\u0521\u0523"+
		"\7]\2\2\u0522\u0520\3\2\2\2\u0522\u0521\3\2\2\2\u0523\u0091\3\2\2\2\u0524"+
		"\u0526\5\u0098M\2\u0525\u0527\7\177\2\2\u0526\u0525\3\2\2\2\u0526\u0527"+
		"\3\2\2\2\u0527\u0528\3\2\2\2\u0528\u052a\7\t\2\2\u0529\u052b\7\177\2\2"+
		"\u052a\u0529\3\2\2\2\u052a\u052b\3\2\2\2\u052b\u053d\3\2\2\2\u052c\u052e"+
		"\5d\63\2\u052d\u052f\7\177\2\2\u052e\u052d\3\2\2\2\u052e\u052f\3\2\2\2"+
		"\u052f\u053a\3\2\2\2\u0530\u0532\7\4\2\2\u0531\u0533\7\177\2\2\u0532\u0531"+
		"\3\2\2\2\u0532\u0533\3\2\2\2\u0533\u0534\3\2\2\2\u0534\u0536\5d\63\2\u0535"+
		"\u0537\7\177\2\2\u0536\u0535\3\2\2\2\u0536\u0537\3\2\2\2\u0537\u0539\3"+
		"\2\2\2\u0538\u0530\3\2\2\2\u0539\u053c\3\2\2\2\u053a\u0538\3\2\2\2\u053a"+
		"\u053b\3\2\2\2\u053b\u053e\3\2\2\2\u053c\u053a\3\2\2\2\u053d\u052c\3\2"+
		"\2\2\u053d\u053e\3\2\2\2\u053e\u053f\3\2\2\2\u053f\u0540\7\n\2\2\u0540"+
		"\u0093\3\2\2\2\u0541\u0542\5\u0098M\2\u0542\u0095\3\2\2\2\u0543\u0544"+
		"\5\u00ba^\2\u0544\u0097\3\2\2\2\u0545\u0546\5\u009aN\2\u0546\u0547\5\u00ba"+
		"^\2\u0547\u0099\3\2\2\2\u0548\u0549\5\u00ba^\2\u0549\u054a\7\31\2\2\u054a"+
		"\u054c\3\2\2\2\u054b\u0548\3\2\2\2\u054c\u054f\3\2\2\2\u054d\u054b\3\2"+
		"\2\2\u054d\u054e\3\2\2\2\u054e\u009b\3\2\2\2\u054f\u054d\3\2\2\2\u0550"+
		"\u0552\7\13\2\2\u0551\u0553\7\177\2\2\u0552\u0551\3\2\2\2\u0552\u0553"+
		"\3\2\2\2\u0553\u0554\3\2\2\2\u0554\u055d\5\u008aF\2\u0555\u0557\7\177"+
		"\2\2\u0556\u0555\3\2\2\2\u0556\u0557\3\2\2\2\u0557\u0558\3\2\2\2\u0558"+
		"\u055a\7\16\2\2\u0559\u055b\7\177\2\2\u055a\u0559\3\2\2\2\u055a\u055b"+
		"\3\2\2\2\u055b\u055c\3\2\2\2\u055c\u055e\5d\63\2\u055d\u0556\3\2\2\2\u055d"+
		"\u055e\3\2\2\2\u055e\u0560\3\2\2\2\u055f\u0561\7\177\2\2\u0560\u055f\3"+
		"\2\2\2\u0560\u0561\3\2\2\2\u0561\u0562\3\2\2\2\u0562\u0563\7\f\2\2\u0563"+
		"\u009d\3\2\2\2\u0564\u0566\7\13\2\2\u0565\u0567\7\177\2\2\u0566\u0565"+
		"\3\2\2\2\u0566\u0567\3\2\2\2\u0567\u0570\3\2\2\2\u0568\u056a\5\u00a6T"+
		"\2\u0569\u056b\7\177\2\2\u056a\u0569\3\2\2\2\u056a\u056b\3\2\2\2\u056b"+
		"\u056c\3\2\2\2\u056c\u056e\7\5\2\2\u056d\u056f\7\177\2\2\u056e\u056d\3"+
		"\2\2\2\u056e\u056f\3\2\2\2\u056f\u0571\3\2\2\2\u0570\u0568\3\2\2\2\u0570"+
		"\u0571\3\2\2\2\u0571\u0572\3\2\2\2\u0572\u0574\5\u0088E\2\u0573\u0575"+
		"\7\177\2\2\u0574\u0573\3\2\2\2\u0574\u0575\3\2\2\2\u0575\u057e\3\2\2\2"+
		"\u0576\u0578\7J\2\2\u0577\u0579\7\177\2\2\u0578\u0577\3\2\2\2\u0578\u0579"+
		"\3\2\2\2\u0579\u057a\3\2\2\2\u057a\u057c\5d\63\2\u057b\u057d\7\177\2\2"+
		"\u057c\u057b\3\2\2\2\u057c\u057d\3\2\2\2\u057d\u057f\3\2\2\2\u057e\u0576"+
		"\3\2\2\2\u057e\u057f\3\2\2\2\u057f\u0580\3\2\2\2\u0580\u0582\7\16\2\2"+
		"\u0581\u0583\7\177\2\2\u0582\u0581\3\2\2\2\u0582\u0583\3\2\2\2\u0583\u0584"+
		"\3\2\2\2\u0584\u0586\5d\63\2\u0585\u0587\7\177\2\2\u0586\u0585\3\2\2\2"+
		"\u0586\u0587\3\2\2\2\u0587\u0588\3\2\2\2\u0588\u0589\7\f\2\2\u0589\u009f"+
		"\3\2\2\2\u058a\u058c\7\31\2\2\u058b\u058d\7\177\2\2\u058c\u058b\3\2\2"+
		"\2\u058c\u058d\3\2\2\2\u058d\u058e\3\2\2\2\u058e\u058f\5\u00b0Y\2\u058f"+
		"\u00a1\3\2\2\2\u0590\u0595\7^\2\2\u0591\u0593\7\177\2\2\u0592\u0591\3"+
		"\2\2\2\u0592\u0593\3\2\2\2\u0593\u0594\3\2\2\2\u0594\u0596\5\u00a4S\2"+
		"\u0595\u0592\3\2\2\2\u0596\u0597\3\2\2\2\u0597\u0595\3\2\2\2\u0597\u0598"+
		"\3\2\2\2\u0598\u05a7\3\2\2\2\u0599\u059b\7^\2\2\u059a\u059c\7\177\2\2"+
		"\u059b\u059a\3\2\2\2\u059b\u059c\3\2\2\2\u059c\u059d\3\2\2\2\u059d\u05a2"+
		"\5d\63\2\u059e\u05a0\7\177\2\2\u059f\u059e\3\2\2\2\u059f\u05a0\3\2\2\2"+
		"\u05a0\u05a1\3\2\2\2\u05a1\u05a3\5\u00a4S\2\u05a2\u059f\3\2\2\2\u05a3"+
		"\u05a4\3\2\2\2\u05a4\u05a2\3\2\2\2\u05a4\u05a5\3\2\2\2\u05a5\u05a7\3\2"+
		"\2\2\u05a6\u0590\3\2\2\2\u05a6\u0599\3\2\2\2\u05a7\u05b0\3\2\2\2\u05a8"+
		"\u05aa\7\177\2\2\u05a9\u05a8\3\2\2\2\u05a9\u05aa\3\2\2\2\u05aa\u05ab\3"+
		"\2\2\2\u05ab\u05ad\7_\2\2\u05ac\u05ae\7\177\2\2\u05ad\u05ac\3\2\2\2\u05ad"+
		"\u05ae\3\2\2\2\u05ae\u05af\3\2\2\2\u05af\u05b1\5d\63\2\u05b0\u05a9\3\2"+
		"\2\2\u05b0\u05b1\3\2\2\2\u05b1\u05b3\3\2\2\2\u05b2\u05b4\7\177\2\2\u05b3"+
		"\u05b2\3\2\2\2\u05b3\u05b4\3\2\2\2\u05b4\u05b5\3\2\2\2\u05b5\u05b6\7`"+
		"\2\2\u05b6\u00a3\3\2\2\2\u05b7\u05b9\7a\2\2\u05b8\u05ba\7\177\2\2\u05b9"+
		"\u05b8\3\2\2\2\u05b9\u05ba\3\2\2\2\u05ba\u05bb\3\2\2\2\u05bb\u05bd\5d"+
		"\63\2\u05bc\u05be\7\177\2\2\u05bd\u05bc\3\2\2\2\u05bd\u05be\3\2\2\2\u05be"+
		"\u05bf\3\2\2\2\u05bf\u05c1\7b\2\2\u05c0\u05c2\7\177\2\2\u05c1\u05c0\3"+
		"\2\2\2\u05c1\u05c2\3\2\2\2\u05c2\u05c3\3\2\2\2\u05c3\u05c4\5d\63\2\u05c4"+
		"\u00a5\3\2\2\2\u05c5\u05c6\5\u00ba^\2\u05c6\u00a7\3\2\2\2\u05c7\u05ca"+
		"\5\u00b4[\2\u05c8\u05ca\5\u00b2Z\2\u05c9\u05c7\3\2\2\2\u05c9\u05c8\3\2"+
		"\2\2\u05ca\u00a9\3\2\2\2\u05cb\u05cd\7\32\2\2\u05cc\u05ce\7\177\2\2\u05cd"+
		"\u05cc\3\2\2\2\u05cd\u05ce\3\2\2\2\u05ce\u05f0\3\2\2\2\u05cf\u05d1\5\u00b0"+
		"Y\2\u05d0\u05d2\7\177\2\2\u05d1\u05d0\3\2\2\2\u05d1\u05d2\3\2\2\2\u05d2"+
		"\u05d3\3\2\2\2\u05d3\u05d5\7\r\2\2\u05d4\u05d6\7\177\2\2\u05d5\u05d4\3"+
		"\2\2\2\u05d5\u05d6\3\2\2\2\u05d6\u05d7\3\2\2\2\u05d7\u05d9\5d\63\2\u05d8"+
		"\u05da\7\177\2\2\u05d9\u05d8\3\2\2\2\u05d9\u05da\3\2\2\2\u05da\u05ed\3"+
		"\2\2\2\u05db\u05dd\7\4\2\2\u05dc\u05de\7\177\2\2\u05dd\u05dc\3\2\2\2\u05dd"+
		"\u05de\3\2\2\2\u05de\u05df\3\2\2\2\u05df\u05e1\5\u00b0Y\2\u05e0\u05e2"+
		"\7\177\2\2\u05e1\u05e0\3\2\2\2\u05e1\u05e2\3\2\2\2\u05e2\u05e3\3\2\2\2"+
		"\u05e3\u05e5\7\r\2\2\u05e4\u05e6\7\177\2\2\u05e5\u05e4\3\2\2\2\u05e5\u05e6"+
		"\3\2\2\2\u05e6\u05e7\3\2\2\2\u05e7\u05e9\5d\63\2\u05e8\u05ea\7\177\2\2"+
		"\u05e9\u05e8\3\2\2\2\u05e9\u05ea\3\2\2\2\u05ea\u05ec\3\2\2\2\u05eb\u05db"+
		"\3\2\2\2\u05ec\u05ef\3\2\2\2\u05ed\u05eb\3\2\2\2\u05ed\u05ee\3\2\2\2\u05ee"+
		"\u05f1\3\2\2\2\u05ef\u05ed\3\2\2\2\u05f0\u05cf\3\2\2\2\u05f0\u05f1\3\2"+
		"\2\2\u05f1\u05f2\3\2\2\2\u05f2\u05f3\7\33\2\2\u05f3\u00ab\3\2\2\2\u05f4"+
		"\u05f7\7\34\2\2\u05f5\u05f8\5\u00ba^\2\u05f6\u05f8\7f\2\2\u05f7\u05f5"+
		"\3\2\2\2\u05f7\u05f6\3\2\2\2\u05f8\u00ad\3\2\2\2\u05f9\u05fe\5|?\2\u05fa"+
		"\u05fc\7\177\2\2\u05fb\u05fa\3\2\2\2\u05fb\u05fc\3\2\2\2\u05fc\u05fd\3"+
		"\2\2\2\u05fd\u05ff\5\u00a0Q\2\u05fe\u05fb\3\2\2\2\u05ff\u0600\3\2\2\2"+
		"\u0600\u05fe\3\2\2\2\u0600\u0601\3\2\2\2\u0601\u00af\3\2\2\2\u0602\u0603"+
		"\5\u00b6\\\2\u0603\u00b1\3\2\2\2\u0604\u0605\t\5\2\2\u0605\u00b3\3\2\2"+
		"\2\u0606\u0607\t\6\2\2\u0607\u00b5\3\2\2\2\u0608\u060b\5\u00ba^\2\u0609"+
		"\u060b\5\u00b8]\2\u060a\u0608\3\2\2\2\u060a\u0609\3\2\2\2\u060b\u00b7"+
		"\3\2\2\2\u060c\u060d\t\7\2\2\u060d\u00b9\3\2\2\2\u060e\u060f\t\b\2\2\u060f"+
		"\u00bb\3\2\2\2\u0610\u0611\t\t\2\2\u0611\u00bd\3\2\2\2\u0612\u0613\t\n"+
		"\2\2\u0613\u00bf\3\2\2\2\u0614\u0615\t\13\2\2\u0615\u00c1\3\2\2\2\u0122"+
		"\u00c3\u00c7\u00ca\u00cd\u00d5\u00d9\u00de\u00e5\u00ea\u00ed\u00f1\u00f5"+
		"\u00f9\u00ff\u0103\u0108\u010d\u0111\u0114\u0116\u011a\u011e\u0123\u0127"+
		"\u012c\u0130\u0139\u013e\u0142\u0146\u014a\u014d\u0151\u015b\u0162\u016f"+
		"\u0173\u0179\u0180\u0185\u0189\u018f\u0193\u0199\u019d\u01a3\u01a7\u01ab"+
		"\u01af\u01b3\u01b7\u01bc\u01c3\u01c7\u01cc\u01d3\u01d9\u01de\u01e4\u01ea"+
		"\u01ee\u01f2\u01f7\u01fb\u0202\u0208\u020b\u0210\u0213\u0217\u021a\u0222"+
		"\u0226\u022a\u022e\u0232\u0237\u023c\u0240\u0245\u0248\u0251\u025a\u025f"+
		"\u026c\u026f\u0277\u027b\u0280\u0285\u0289\u028e\u0294\u0299\u02a0\u02a4"+
		"\u02a8\u02aa\u02ae\u02b0\u02b4\u02b6\u02bc\u02c2\u02c6\u02c9\u02cc\u02d0"+
		"\u02d6\u02da\u02dd\u02e0\u02e6\u02e9\u02ec\u02f0\u02f6\u02f9\u02fc\u0300"+
		"\u0304\u0308\u030a\u030e\u0310\u0313\u0317\u0319\u031f\u0323\u0327\u032b"+
		"\u032e\u0333\u0338\u033d\u0342\u0348\u034c\u034e\u0352\u0356\u0358\u035a"+
		"\u0369\u0373\u037d\u0382\u0386\u038d\u0392\u0397\u039b\u039f\u03a3\u03a6"+
		"\u03a8\u03ad\u03b1\u03b5\u03b9\u03bd\u03c1\u03c4\u03c6\u03cb\u03cf\u03d4"+
		"\u03d9\u03dd\u03e4\u03eb\u03ef\u03f3\u0402\u0405\u0412\u0414\u0419\u041e"+
		"\u0422\u0425\u042c\u0430\u0434\u043b\u043f\u0443\u0449\u044d\u0451\u0454"+
		"\u0458\u045e\u0462\u0466\u046c\u0470\u0474\u047a\u047e\u0482\u0488\u048c"+
		"\u0490\u0498\u04a0\u04a6\u04aa\u04ae\u04b2\u04b6\u04b9\u04bf\u04c4\u04c9"+
		"\u04ce\u04d3\u04d8\u04db\u04df\u04e3\u04e9\u04ee\u04f2\u04f5\u04ff\u0503"+
		"\u0507\u0509\u050d\u0511\u0515\u0519\u051c\u0522\u0526\u052a\u052e\u0532"+
		"\u0536\u053a\u053d\u054d\u0552\u0556\u055a\u055d\u0560\u0566\u056a\u056e"+
		"\u0570\u0574\u0578\u057c\u057e\u0582\u0586\u058c\u0592\u0597\u059b\u059f"+
		"\u05a4\u05a6\u05a9\u05ad\u05b0\u05b3\u05b9\u05bd\u05c1\u05c9\u05cd\u05d1"+
		"\u05d5\u05d9\u05dd\u05e1\u05e5\u05e9\u05ed\u05f0\u05f7\u05fb\u0600\u060a";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}