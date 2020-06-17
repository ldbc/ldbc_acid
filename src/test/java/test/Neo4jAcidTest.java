package test;

import bolt.BoltDriver;
import org.junit.BeforeClass;

public class Neo4jAcidTest extends AcidTest<BoltDriver> {

    public Neo4jAcidTest() {
        super(new BoltDriver(7687));
    }

    @BeforeClass
    public static void setUp() {}

}
