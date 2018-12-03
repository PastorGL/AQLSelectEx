package io.github.pastorgl.aqlselectex;

import com.aerospike.client.command.ParticleType;
import com.aerospike.client.query.*;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.*;

import static io.github.pastorgl.aqlselectex.AQLSelectExLexer.CLOSE_PAR;
import static io.github.pastorgl.aqlselectex.AQLSelectExLexer.OPEN_PAR;

public class AQLSelectExListenerImpl extends AQLSelectExBaseListener {
    private final Statement statement;
    private final Map<String, Map<String, Integer>> schema;

    private Map<String, Integer> binTypes;

    private List<PredExp> where = new ArrayList<>();

    private Deque<ParseTree> whereOpStack = new LinkedList<>();
    private List<ParseTree> predExpStack = new ArrayList<>();

    public AQLSelectExListenerImpl(Statement statement, Map<String, Map<String, Integer>> schema) {
        this.statement = statement;
        this.schema = schema;
    }

    @Override
    public void exitSelect_stmt(AQLSelectExParser.Select_stmtContext ctx) {
        if (ctx.STAR() == null) {
            List<String> binNames = new ArrayList<>();
            for (AQLSelectExParser.Column_nameContext cnc : ctx.column_name()) {
                binNames.add(stripNameQuotes(cnc.bin_name().IDENTIFIER().getText()));
            }
            statement.setBinNames(binNames.toArray(new String[0]));
        }

        AQLSelectExParser.From_setContext fsc = ctx.from_set();
        statement.setNamespace(fsc.ns_name().getText());
        String fqns = statement.getNamespace();
        if (fsc.set_name() != null) {
            statement.setSetName(fsc.set_name().getText());
            fqns += "." + statement.getSetName();
        }

        this.binTypes = schema.get(fqns);
        if (binTypes == null) {
            binTypes = Collections.emptyMap();
        }

        while (!whereOpStack.isEmpty()) {
            predExpStack.add(whereOpStack.pop());
        }

        for (ParseTree whereExpr : predExpStack) {
            if (whereExpr instanceof AQLSelectExParser.Atomic_exprContext) {
                AQLSelectExParser.Atomic_exprContext atomicExpr = (AQLSelectExParser.Atomic_exprContext) whereExpr;

                AQLSelectExParser.Equality_opContext equalityOp = atomicExpr.equality_op();
                AQLSelectExParser.Comparison_opContext comparisonOp = atomicExpr.comparison_op();
                TerminalNode numericLiteral = atomicExpr.NUMERIC_LITERAL();

                AQLSelectExParser.Meta_nameContext metaName = atomicExpr.meta_name();
                // meta_name equality_op NUMERIC_LITERAL
                if (equalityOp != null && metaName != null && numericLiteral != null) {
                    addPredExpMetaValue(metaName, numericLiteral.getText());

                    addNumericEqualityOp(equalityOp);
                    continue;
                }

                // meta_name comparison_op NUMERIC_LITERAL
                if (comparisonOp != null && metaName != null && numericLiteral != null) {
                    addPredExpMetaValue(metaName, numericLiteral.getText());

                    addNumericComparisonOp(comparisonOp);
                    continue;
                }

                AQLSelectExParser.Column_nameContext columnName = atomicExpr.column_name();
                String binName = columnName.getText();

                // column_name equality_op STRING_LITERAL
                TerminalNode stringLiteral = atomicExpr.STRING_LITERAL();
                if (equalityOp != null && stringLiteral != null) {
                    addPredExpBinValue(binName, stripStringQuotes(stringLiteral.getText()));

                    addStringEqualityOp(equalityOp);
                    continue;
                }

                // column_name equality_op NUMERIC_LITERAL
                if (equalityOp != null && numericLiteral != null) {
                    addPredExpBinValue(binName, numericLiteral.getText());

                    addNumericEqualityOp(equalityOp);
                    continue;
                }

                // column_name comparison_op NUMERIC_LITERAL
                if (comparisonOp != null && numericLiteral != null) {
                    addPredExpBinValue(binName, numericLiteral.getText());

                    addNumericComparisonOp(comparisonOp);
                    continue;
                }

                AQLSelectExParser.Iter_exprContext iterExpr = atomicExpr.iter_expr();
                if (iterExpr != null) {
                    String varName = iterExpr.var_name().getText();
                    AQLSelectExParser.Equality_opContext iterEqualityOp = iterExpr.equality_op();
                    TerminalNode iterStringLiteral = iterExpr.STRING_LITERAL();
                    TerminalNode iterNumericLiteral = iterExpr.NUMERIC_LITERAL();

                    AQLSelectExParser.Map_opContext mapOp = atomicExpr.map_op();
                    // column_name map_op iter_expr
                    if (mapOp != null) {
                        if (mapOp.K_MAPKEYS() != null) {
                            if (mapOp.K_ANY() != null) {
                                where.add(PredExp.mapKeyIterateOr(varName));
                            } else {
                                where.add(PredExp.mapKeyIterateAnd(varName));
                            }
                        }
                        if (mapOp.K_MAPVALUES() != null) {
                            if (mapOp.K_ANY() != null) {
                                where.add(PredExp.mapValIterateOr(varName));
                            } else {
                                where.add(PredExp.mapValIterateAnd(varName));
                            }
                        }
                    }

                    AQLSelectExParser.List_opContext listOp = atomicExpr.list_op();
                    // column_name list_op iter_expr
                    if (listOp != null) {
                        if (listOp.K_ANY() != null) {
                            where.add(PredExp.listIterateOr(varName));
                        } else {
                            where.add(PredExp.listIterateAnd(varName));
                        }
                    }

                    if (iterStringLiteral != null) {
                        addPredExpBinVar(binName, varName, stripStringQuotes(iterStringLiteral.getText()));

                        addStringEqualityOp(iterEqualityOp);
                    }
                    if (iterNumericLiteral != null) {
                        addPredExpBinVar(binName, varName, iterNumericLiteral.getText());

                        if (iterEqualityOp != null) {
                            addNumericEqualityOp(iterEqualityOp);
                        }

                        AQLSelectExParser.Comparison_opContext iterComparisonOp = iterExpr.comparison_op();
                        if (iterComparisonOp != null) {
                            addNumericComparisonOp(iterComparisonOp);
                        }
                    }

                    continue;
                }

                AQLSelectExParser.Geo_opContext geoOp = atomicExpr.geo_op();
                // column_name geo_op cast_expr
                if (geoOp != null) {
                    if (atomicExpr.cast_expr().type_name().K_GEOJSON() == null) {
                        throw new RuntimeException("Expected GEOJSON type cast at input index " + ctx.getRuleIndex());
                    }

                    String geo = stripStringQuotes(atomicExpr.cast_expr().STRING_LITERAL().getText());
                    addPredExpBinValue(binName, geo);

                    if (geoOp.K_CONTAINS() != null) {
                        where.add(PredExp.geoJSONContains());
                    }
                    if (geoOp.K_WITHIN() != null) {
                        where.add(PredExp.geoJSONWithin());
                    }

                    continue;
                }

                // column_name regex_op STRING_LITERAL
                if (atomicExpr.regex_op() != null) {
                    String pattern = stripStringQuotes(stringLiteral.getText());
                    int regexFlags = RegexFlag.NEWLINE;
                    if (pattern.startsWith("/")) {
                        int lastSlash = pattern.lastIndexOf('/');

                        String patternFlags = pattern.substring(lastSlash).toLowerCase();
                        regexFlags |= patternFlags.contains("i") ? RegexFlag.ICASE : RegexFlag.NONE;
                        regexFlags |= patternFlags.contains("e") ? RegexFlag.EXTENDED : RegexFlag.NONE;
                        if (patternFlags.contains("s")) {
                            regexFlags &= ~RegexFlag.NEWLINE;
                        } else {
                            regexFlags |= RegexFlag.NEWLINE;
                        }
                        regexFlags |= patternFlags.contains("?") ? RegexFlag.NOSUB : RegexFlag.NONE;

                        pattern = pattern.substring(1, lastSlash);
                    }

                    addPredExpBinValue(binName, pattern);
                    where.add(PredExp.stringRegex(regexFlags));
                }

                continue;
            }

            if (whereExpr instanceof AQLSelectExParser.Logic_opContext) {
                AQLSelectExParser.Logic_opContext logicOp = (AQLSelectExParser.Logic_opContext)whereExpr;

                if (logicOp.K_NOT() != null) {
                    where.add(PredExp.not());
                }
                if (logicOp.K_AND() != null) {
                    where.add(PredExp.and(2));
                }
                if (logicOp.K_OR() != null) {
                    where.add(PredExp.or(2));
                }
            }
        }

        if (where.size() > 0) {
            statement.setPredExp(where.toArray(new PredExp[0]));
        }
    }

    private void addNumericComparisonOp(AQLSelectExParser.Comparison_opContext comparisonOp) {
        if (comparisonOp.GT() != null) {
            where.add(PredExp.integerGreater());
        }
        if (comparisonOp.GT_EQ() != null) {
            where.add(PredExp.integerGreaterEq());
        }
        if (comparisonOp.LT() != null) {
            where.add(PredExp.integerLess());
        }
        if (comparisonOp.LT_EQ() != null) {
            where.add(PredExp.integerLessEq());
        }
    }

    private void addNumericEqualityOp(AQLSelectExParser.Equality_opContext equalityOp) {
        if (equalityOp.EQ() != null || equalityOp.EQ2() != null) {
            where.add(PredExp.integerEqual());
        }
        if (equalityOp.NOT_EQ1() != null || equalityOp.NOT_EQ2() != null) {
            where.add(PredExp.integerUnequal());
        }
    }

    private void addStringEqualityOp(AQLSelectExParser.Equality_opContext equalityOp) {
        if (equalityOp.EQ() != null || equalityOp.EQ2() != null) {
            where.add(PredExp.stringEqual());
        }
        if (equalityOp.NOT_EQ1() != null || equalityOp.NOT_EQ2() != null) {
            where.add(PredExp.stringUnequal());
        }
    }

    private String stripNameQuotes(String sqlName) {
        if (sqlName == null) {
            return null;
        }
        String name = sqlName;
        // escape character : "
        if (name.length() >= 2) {
            if ((name.charAt(0) == '"') && (name.charAt(name.length() - 1) == '"')) {
                name = name.substring(1, name.length() - 1);
            }
        }
        return name;
    }

    private String stripStringQuotes(String sqlString) {
        if (sqlString == null) {
            return null;
        }
        String string = sqlString;
        // escape character : '
        if ((string.charAt(0) == '\'') && (string.charAt(string.length() - 1) == '\'')) {
            string = string.substring(1, string.length() - 1);
        }
        return string;
    }

    @Override
    public void exitWhere_expr(AQLSelectExParser.Where_exprContext ctx) {
        int i = 0;
        try {
            // doing Shunting Yard
            List<ParseTree> children = ctx.children;
            for (; i < children.size(); i++) {
                ParseTree child = children.get(i);
                if (child instanceof AQLSelectExParser.Logic_opContext) {
                    AQLSelectExParser.Logic_opContext logic = (AQLSelectExParser.Logic_opContext) child;
                    while (!whereOpStack.isEmpty() && isHigherOp(logic, whereOpStack.peek())) {
                        predExpStack.add(whereOpStack.pop());
                    }
                    whereOpStack.push(logic);
                } else if (child instanceof TerminalNode) {
                    TerminalNode paren = (TerminalNode) child;

                    int type = paren.getSymbol().getType();
                    if (type == OPEN_PAR) {
                        whereOpStack.push(paren);
                    } else {
                        ParseTree p;
                        while (!(((p = whereOpStack.peek()) instanceof TerminalNode) && ((TerminalNode) p).getSymbol().getType() != CLOSE_PAR)) {
                            predExpStack.add(whereOpStack.pop());
                        }
                        whereOpStack.pop();
                    }
                } else {
                    predExpStack.add(child);
                }
            }
        } catch (RuntimeException e) {
            throw new RuntimeException("Mismatched parentheses at WHERE token #" + i, e);
        }
    }

    private boolean isHigherOp(AQLSelectExParser.Logic_opContext ctx1, ParseTree ctx2) {
        if (!(ctx2 instanceof AQLSelectExParser.Logic_opContext)) {
            return false;
        }

        AQLSelectExParser.Logic_opContext other = (AQLSelectExParser.Logic_opContext) ctx2;

        // NOT > *
        if (other.K_NOT() != null) {
            return true;
        }

        // NOT > AND > *
        if ((other.K_AND() != null) && (ctx1.K_NOT() == null)) {
            return true;
        }

        // * >= OR
        if ((other.K_OR() != null) && (ctx1.K_OR() != null)) {
            return true;
        }

        return false;
    }

    @Override
    public void exitIndex_expr(AQLSelectExParser.Index_exprContext ctx) {
        Filter filter = null;
        String binName = ctx.column_name().getText();

        String index = (ctx.index_type() == null) ? IndexCollectionType.DEFAULT.name() : ctx.index_type().getText();

        AQLSelectExParser.Index_opContext indexOp = ctx.index_op();
        if (indexOp != null) {
            if (ctx.NUMERIC_LITERAL() != null) {
                if (indexOp.EQ() != null || indexOp.EQ2() != null) {
                    filter = Filter.equal(binName, new Double(ctx.NUMERIC_LITERAL().getText()).longValue());
                }
                if (indexOp.K_CONTAINS() != null) {
                    filter = Filter.contains(binName, IndexCollectionType.valueOf(index), new Double(ctx.NUMERIC_LITERAL().getText()).longValue());
                }
            }

            if (ctx.STRING_LITERAL() != null) {
                String string = stripStringQuotes(ctx.STRING_LITERAL().getText());
                if (indexOp.EQ() != null || indexOp.EQ2() != null) {
                    filter = Filter.equal(binName, string);
                }
                if (indexOp.K_CONTAINS() != null) {
                    filter = Filter.contains(binName, IndexCollectionType.valueOf(index), string);
                }
            }
        }

        AQLSelectExParser.Geo_opContext geoOp = ctx.geo_op();
        if (geoOp != null) {
            AQLSelectExParser.Cast_exprContext castExpr = ctx.cast_expr();
            if (castExpr != null) {
                if (castExpr.type_name().K_GEOJSON() == null) {
                    throw new RuntimeException("Expected GEOJSON type cast at input index " + ctx.getRuleIndex());
                }

                String string = stripStringQuotes(castExpr.STRING_LITERAL().getText());
                if (geoOp.K_CONTAINS() != null) {
                    filter = Filter.geoContains(binName, IndexCollectionType.valueOf(index), string);
                }
                if (geoOp.K_WITHIN() != null) {
                    filter = Filter.geoWithinRegion(binName, IndexCollectionType.valueOf(index), string);
                }
            }
        }

        AQLSelectExParser.Between_exprContext betweenExpr = ctx.between_expr();
        if (betweenExpr != null) {
            filter = Filter.range(
                    binName,
                    IndexCollectionType.valueOf(index),
                    new Double(betweenExpr.low.getText()).longValue(),
                    new Double(betweenExpr.high.getText()).longValue()
            );
        }

        statement.setFilter(filter);
    }

    private void addPredExpMetaValue(AQLSelectExParser.Meta_nameContext metaName, String raw) {
        if (metaName.K_DIGEST() != null) {
            where.add(PredExp.recDigestModulo(new Double(metaName.NUMERIC_LITERAL().getText()).intValue()));
        }

        if (metaName.K_LUT() != null) {
            where.add(PredExp.recLastUpdate());
        }

        if (metaName.K_RECSIZE() != null) {
            where.add(PredExp.recDeviceSize());
        }

        if (metaName.K_TTL() != null) {
            where.add(PredExp.recVoidTime());
        }

        where.add(PredExp.integerValue(new Long(raw)));
    }

    private void addPredExpBinValue(String bin, String raw) {
        int binType = binTypes.getOrDefault(bin, ParticleType.STRING);

        switch (binType) {
            case ParticleType.INTEGER: {
                where.add(PredExp.integerBin(bin));
                where.add(PredExp.integerValue(new Long(raw)));
            }
            break;
            case ParticleType.DOUBLE: {
                where.add(PredExp.integerBin(bin));
                where.add(PredExp.integerValue(new Double(raw).longValue()));
            }
            break;
            case ParticleType.STRING: {
                where.add(PredExp.stringBin(bin));
                where.add(PredExp.stringValue(raw));
            }
            break;
            case ParticleType.GEOJSON: {
                where.add(PredExp.geoJSONBin(bin));
                where.add(PredExp.geoJSONValue(raw));
            }
            break;
        }
    }

    private void addPredExpBinVar(String bin, String var, String raw) {
        int binType = binTypes.getOrDefault(bin, ParticleType.STRING);
        int varType = binTypes.getOrDefault(bin + "." + var, ParticleType.STRING);

        switch (binType) {
            case ParticleType.MAP: {
                where.add(PredExp.mapBin(bin));
            }
            break;
            case ParticleType.LIST: {
                where.add(PredExp.listBin(bin));
            }
            break;
        }

        switch (varType) {
            case ParticleType.STRING: {
                where.add(PredExp.stringVar(var));
                where.add(PredExp.stringValue(raw));
            }
            break;
            case ParticleType.INTEGER: {
                where.add(PredExp.integerVar(var));
                where.add(PredExp.integerValue(new Long(raw)));
            }
            break;
            case ParticleType.DOUBLE: {
                where.add(PredExp.integerVar(var));
                where.add(PredExp.integerValue(new Double(raw).longValue()));
            }
            break;
            case ParticleType.GEOJSON: {
                where.add(PredExp.geoJSONVar(var));
                where.add(PredExp.geoJSONValue(raw));
            }
            break;
        }
    }
}
