package test;

import neo4j.Neo4jDriver;
import org.junit.BeforeClass;

public class Neo4jAcidTest extends AcidTest<Neo4jDriver> {

    public Neo4jAcidTest() {
        super(new Neo4jDriver(17687));
    }

    @BeforeClass
    public static void setUp() {}

}
