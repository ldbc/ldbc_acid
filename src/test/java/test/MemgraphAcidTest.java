package test;

import neo4j.Neo4jDriver;
import org.junit.BeforeClass;

public class MemgraphAcidTest extends AcidTest<Neo4jDriver> {

    public MemgraphAcidTest() {
        super(new Neo4jDriver(17687));
    }

    @BeforeClass
    public static void setUp() {}

}
