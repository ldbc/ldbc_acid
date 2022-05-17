package test;

import tigergraph.TigerGraphDriver;
import org.junit.BeforeClass;

public class TigerGraphTest extends AcidTest<TigerGraphDriver> {

    public TigerGraphTest() {
        super(new TigerGraphDriver("http://localhost:9000", "ldbc_acid"));
    }

    @BeforeClass
    public static void setUp() {}

}
