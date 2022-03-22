package test;

import tigergraph.TigergraphDriver;
import org.junit.BeforeClass;

public class TigergraphTest extends AcidTest<TigergraphDriver> {

    public TigergraphTest() {
        super(new TigergraphDriver("http://localhost:9000", "ldbc_acid"));
    }

    @BeforeClass
    public static void setUp() {}

}
