package test;

import bolt.BoltDriver;
import org.junit.BeforeClass;

public class MemgraphAcidTest extends AcidTest<BoltDriver> {

    public MemgraphAcidTest() {
        super(new BoltDriver(17687));
    }

    @BeforeClass
    public static void setUp() {}

}
