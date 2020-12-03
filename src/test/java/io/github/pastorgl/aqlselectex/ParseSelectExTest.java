package io.github.pastorgl.aqlselectex;

import com.aerospike.client.command.Command;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;

import static com.aerospike.client.command.Command.MSG_TOTAL_HEADER_SIZE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ParseSelectExTest {

    private final HashMap FOO_BAR_BAZ = new HashMap() {{
        put("namespace1.sett", new HashMap() {{
            put("foo", ParticleType.INTEGER);
            put("bar", ParticleType.DOUBLE);
            put("baz", ParticleType.STRING);
        }});
    }};
    private final HashMap GEOJSON = new HashMap() {{
        put("namespace1.sett", new HashMap() {{
            put("gj", ParticleType.GEOJSON);
        }});
    }};
    private final HashMap LIST_MAP = new HashMap() {{
        put("namespace1.sett", new HashMap() {{
            put("li", ParticleType.LIST);
            put("ma", ParticleType.MAP);
            put("ma.ko", ParticleType.INTEGER);
        }});
    }};

    private final String[] expectedBins = new String[]{"foo", "bar", "baz"};

    @Test
    public void testBasicsAndNestingAndPrecedence() throws Exception {
        AQLSelectEx selectEx = AQLSelectEx.forSchema(FOO_BAR_BAZ);

        Statement statement = selectEx.fromString("  SELECT foo,bar,baz FROM namespace1.sett WITH(baz='a') WHERE (foo>2 AND (bar <=3 OR foo>5) AND bar >3) OR NOT (foo<=100) ");

        assertEquals("sett", statement.getSetName());
        assertEquals("namespace1", statement.getNamespace());
        assertArrayEquals(expectedBins, statement.getBinNames());
        Filter expectedFilter = Filter.equal("baz", "a");
        assertEquals(expectedFilter, statement.getFilter());

        PredExp[] predExp = statement.getPredExp();
        assertEquals(20, predExp.length);

        Statement reference = new Statement();
        reference.setSetName("sett");
        reference.setNamespace("namespace1");
        reference.setBinNames(expectedBins);
        reference.setFilter(expectedFilter);

        /* check equivalence of
        infix: (foo>2 AND (bar <=3 OR foo>5) AND bar >3) OR NOT (foo<=100)
        and PNR: foo 2 > bar 3 <= foo 5 > or() and() bar 3 > and() foo 100 <= not() or()
         */
        reference.setPredExp(
                PredExp.integerBin("foo")
                , PredExp.integerValue(2)
                , PredExp.integerGreater()
                , PredExp.integerBin("bar")
                , PredExp.integerValue(3)
                , PredExp.integerLessEq()
                , PredExp.integerBin("foo")
                , PredExp.integerValue(5)
                , PredExp.integerGreater()
                , PredExp.or(2)
                , PredExp.and(2)
                , PredExp.integerBin("bar")
                , PredExp.integerValue(3)
                , PredExp.integerGreater()
                , PredExp.and(2)
                , PredExp.integerBin("foo")
                , PredExp.integerValue(100)
                , PredExp.integerLessEq()
                , PredExp.not()
                , PredExp.or(2));

        assertStatementEquals(reference, statement, 512);
    }

    @Test
    public void indexesFilterTest() throws Exception {
        AQLSelectEx selectEx = AQLSelectEx.forSchema(FOO_BAR_BAZ);

        Statement statement = selectEx.fromString("SELECT foo,bar,baz FROM namespace1.sett WITH MAPKEYS (baz CONTAINS 'a')");

        Filter expectedFilter = Filter.contains("baz", IndexCollectionType.MAPKEYS, "a");
        assertEquals(expectedFilter, statement.getFilter());

        statement = selectEx.fromString("SELECT foo,bar,baz FROM namespace1.sett WITH MAPVALUES (baz CONTAINS 'a')");

        expectedFilter = Filter.contains("baz", IndexCollectionType.MAPVALUES, "a");
        assertEquals(expectedFilter, statement.getFilter());

        statement = selectEx.fromString("SELECT foo,bar,baz FROM namespace1.sett WITH MAPVALUES (foo CONTAINS 100)");

        expectedFilter = Filter.contains("foo", IndexCollectionType.MAPVALUES, 100);
        assertEquals(expectedFilter, statement.getFilter());

        statement = selectEx.fromString("SELECT foo,bar,baz FROM namespace1.sett WITH (baz CONTAINS CAST('a' AS GEOJSON))");

        expectedFilter = Filter.geoContains("baz", "a");
        assertEquals(expectedFilter, statement.getFilter());

        statement = selectEx.fromString("SELECT foo,bar,baz FROM namespace1.sett WITH (baz WITHIN CAST('a' AS GEOJSON))");

        expectedFilter = Filter.geoWithinRegion("baz", "a");
        assertEquals(expectedFilter, statement.getFilter());

        statement = selectEx.fromString("SELECT foo,bar,baz FROM namespace1.sett WITH (foo = 200)");

        expectedFilter = Filter.equal("foo", 200);
        assertEquals(expectedFilter, statement.getFilter());

        statement = selectEx.fromString("SELECT foo,bar,baz FROM namespace1.sett WITH (foo BETWEEN 100 AND 200)");

        expectedFilter = Filter.range("foo", 100, 200);
        assertEquals(expectedFilter, statement.getFilter());

        statement = selectEx.fromString("SELECT foo,bar,baz FROM namespace1.sett WITH (PK = 'thisisaprimarykey')");

        expectedFilter = Filter.equal("PK", "thisisaprimarykey");
        assertEquals(expectedFilter, statement.getFilter());
    }

    @Test
    public void whereAtomicsTest() throws Exception {
        AQLSelectEx selectEx = AQLSelectEx.forSchema(FOO_BAR_BAZ);

        Statement statement = selectEx.fromString("SELECT foo,bar,baz FROM namespace1.sett WHERE foo=-10");
        Statement reference = new Statement();
        reference.setSetName("sett");
        reference.setNamespace("namespace1");
        reference.setBinNames(expectedBins);
        reference.setPredExp(
                PredExp.integerBin("foo")
                , PredExp.integerValue(-10)
                , PredExp.integerEqual()
        );

        assertStatementEquals(reference, statement, 200);

        statement = selectEx.fromString("SELECT foo,bar,baz FROM namespace1.sett WHERE bar!=-100.000");
        reference.setPredExp(
                PredExp.integerBin("bar")
                , PredExp.integerValue(-100)
                , PredExp.integerUnequal()
        );

        assertStatementEquals(reference, statement, 200);

        statement = selectEx.fromString("SeLECT foo,bar,baz from namespace1.sett where baz like '/sparta.*/ie?'");
        reference.setPredExp(
                PredExp.stringBin("baz")
                , PredExp.stringValue("sparta.*")
                , PredExp.stringRegex(RegexFlag.EXTENDED | RegexFlag.ICASE | RegexFlag.NOSUB | RegexFlag.NEWLINE)
        );

        assertStatementEquals(reference, statement, 200);

        statement = selectEx.fromString("SeLECT foo,bar,baz from namespace1.sett where baz like '/sparta.*/s'");
        reference.setPredExp(
                PredExp.stringBin("baz")
                , PredExp.stringValue("sparta.*")
                , PredExp.stringRegex(RegexFlag.NONE)
        );

        assertStatementEquals(reference, statement, 200);

        statement = selectEx.fromString("SeLECT foo,bar,baz from namespace1.sett where baz like 'sparta.*'");
        reference.setPredExp(
                PredExp.stringBin("baz")
                , PredExp.stringValue("sparta.*")
                , PredExp.stringRegex(RegexFlag.NEWLINE)
        );

        assertStatementEquals(reference, statement, 200);
    }

    @Test
    public void whereGeoJsonTest() throws Exception {
        AQLSelectEx selectEx = AQLSelectEx.forSchema(GEOJSON);

        Statement reference = new Statement();
        reference.setSetName("sett");
        reference.setNamespace("namespace1");
        reference.setBinNames("gj");

        Statement statement = selectEx.fromString("SeLECT gj from namespace1.sett where gj contains CAST('{\"type\": \"Point\", \"coordinates\": [0.0, 0.0]}' AS GEOJSON)");
        reference.setPredExp(
                PredExp.geoJSONBin("gj")
                , PredExp.geoJSONValue("{\"type\": \"Point\", \"coordinates\": [0.0, 0.0]}")
                , PredExp.geoJSONContains()
        );

        assertStatementEquals(reference, statement, 400);

        statement = selectEx.fromString("SeLECT gj from namespace1.sett where gj WITHIN CAST('{\"type\": \"Polygon\", \"coordinates\": [0.0, 0.0],[1.0, 0.0],[1.0, 1.0],[0.0, 1.0],[0.0, 0.0]}' AS GEOJSON)");
        reference.setPredExp(
                PredExp.geoJSONBin("gj")
                , PredExp.geoJSONValue("{\"type\": \"Polygon\", \"coordinates\": [0.0, 0.0],[1.0, 0.0],[1.0, 1.0],[0.0, 1.0],[0.0, 0.0]}")
                , PredExp.geoJSONWithin()
        );

        assertStatementEquals(reference, statement, 400);
    }

    @Test
    public void whereComplexTypesTest() throws Exception {
        AQLSelectEx selectEx = AQLSelectEx.forSchema(LIST_MAP);

        Statement reference = new Statement();
        reference.setSetName("sett");
        reference.setNamespace("namespace1");
        reference.setBinNames("li", "ma");

        Statement statement = selectEx.fromString("SeLECT li,ma from namespace1.sett where li CONTAINS (ke='aaa')");
        reference.setPredExp(
                PredExp.listIterateAnd("ke"),
                PredExp.listBin("li"),
                PredExp.stringVar("ke"),
                PredExp.stringValue("aaa"),
                PredExp.stringEqual()
        );

        assertStatementEquals(reference, statement, 400);

        statement = selectEx.fromString("SeLECT li,ma from namespace1.sett where li ANY CONTAINS (ke='aaa')");
        reference.setPredExp(
                PredExp.listIterateOr("ke"),
                PredExp.listBin("li"),
                PredExp.stringVar("ke"),
                PredExp.stringValue("aaa"),
                PredExp.stringEqual()
        );

        assertStatementEquals(reference, statement, 400);

        statement = selectEx.fromString("SeLECT li,ma from namespace1.sett where ma MAPVALUES (ke='aaa')");
        reference.setPredExp(
                PredExp.mapValIterateAnd("ke"),
                PredExp.mapBin("ma"),
                PredExp.stringVar("ke"),
                PredExp.stringValue("aaa"),
                PredExp.stringEqual()
        );

        assertStatementEquals(reference, statement, 400);

        statement = selectEx.fromString("SeLECT li,ma from namespace1.sett where ma ANY MAPVALUES (ke='aaa')");
        reference.setPredExp(
                PredExp.mapValIterateOr("ke"),
                PredExp.mapBin("ma"),
                PredExp.stringVar("ke"),
                PredExp.stringValue("aaa"),
                PredExp.stringEqual()
        );

        assertStatementEquals(reference, statement, 400);

        statement = selectEx.fromString("SeLECT li,ma from namespace1.sett where ma MAPKEYS (ke='aaa')");
        reference.setPredExp(
                PredExp.mapKeyIterateAnd("ke"),
                PredExp.mapBin("ma"),
                PredExp.stringVar("ke"),
                PredExp.stringValue("aaa"),
                PredExp.stringEqual()
        );

        assertStatementEquals(reference, statement, 400);

        statement = selectEx.fromString("SeLECT li,ma from namespace1.sett where ma ANY MAPKEYS (ke='aaa')");
        reference.setPredExp(
                PredExp.mapKeyIterateOr("ke"),
                PredExp.mapBin("ma"),
                PredExp.stringVar("ke"),
                PredExp.stringValue("aaa"),
                PredExp.stringEqual()
        );

        assertStatementEquals(reference, statement, 400);

        statement = selectEx.fromString("SeLECT li,ma from namespace1.sett where ma ANY MAPKEYS (ko!=999)");
        reference.setPredExp(
                PredExp.mapKeyIterateOr("ko"),
                PredExp.mapBin("ma"),
                PredExp.integerVar("ko"),
                PredExp.integerValue(999),
                PredExp.integerUnequal()
        );

        assertStatementEquals(reference, statement, 400);

        statement = selectEx.fromString("SeLECT li,ma from namespace1.sett where ma ANY MAPKEYS (ko<=999)");
        reference.setPredExp(
                PredExp.mapKeyIterateOr("ko"),
                PredExp.mapBin("ma"),
                PredExp.integerVar("ko"),
                PredExp.integerValue(999),
                PredExp.integerLessEq()
        );

        assertStatementEquals(reference, statement, 400);
    }

    private void assertStatementEquals(Statement reference, Statement statement, int size) {
        Command command = new Command(0, 0, 0) {
            @Override
            protected void sizeBuffer() {
            }
        };
        byte[] buf;
        byte[] ref;
        buf = new byte[size];
        command.dataBuffer = buf;
        command.setQuery(new QueryPolicy(), statement, false, null);

        ref = new byte[size];
        command.dataBuffer = ref;
        command.setQuery(new QueryPolicy(), reference, false, null);

        assertArrayEquals(Arrays.copyOfRange(ref, MSG_TOTAL_HEADER_SIZE, size), Arrays.copyOfRange(buf, MSG_TOTAL_HEADER_SIZE, size));
    }
}
