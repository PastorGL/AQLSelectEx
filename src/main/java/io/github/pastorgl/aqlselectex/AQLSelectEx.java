package io.github.pastorgl.aqlselectex;

import com.aerospike.client.query.Statement;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.util.HashMap;
import java.util.Map;

public class AQLSelectEx {
    private static Map<Map<String, Map<String, Integer>>, AQLSelectEx> INSTANCES = new HashMap<>();
    private Map<String, Map<String, Integer>> schema;

    private AQLSelectEx(Map<String, Map<String, Integer>> schema) {
        this.schema = schema;
    }

    public static AQLSelectEx forSchema(Map<String, Map<String, Integer>> schema) {
        AQLSelectEx instance;
        if (INSTANCES.containsKey(schema)) {
            instance = INSTANCES.get(schema);
        } else {
            instance = new AQLSelectEx(schema);
            INSTANCES.put(schema, instance);
        }

        return instance;
    }

    public Statement fromString(String select) throws Exception {
        Statement statement = new Statement();

        CharStream cs = CharStreams.fromString(select);

        AQLSelectExLexer lexer = new AQLSelectExLexer(cs);
        AQLSelectExParser parser = new AQLSelectExParser(new CommonTokenStream(lexer));

        AQLSelectExErrorListener errors = new AQLSelectExErrorListener();
        parser.addErrorListener(errors);

        parser.addParseListener(new AQLSelectExListenerImpl(statement, this.schema));

        parser.parse();

        if (errors.hasError()) {
            throw errors.exception();
        }

        return statement;
    }
}
