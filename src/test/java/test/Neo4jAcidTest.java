package test;

import neo4j.Neo4jEmbeddedDb;
import neo4j.Neo4jTestDriver;
import org.junit.BeforeClass;

public class Neo4jAcidTest extends AcidTest<Neo4jTestDriver> {

    @BeforeClass
    public static void setUp() {
        Neo4jEmbeddedDb db = new Neo4jEmbeddedDb();
    }

    public Neo4jAcidTest() {
        super(new Neo4jTestDriver());
    }

}
