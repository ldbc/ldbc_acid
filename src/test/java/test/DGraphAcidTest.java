package test;

import dgraph.DGraphDriver;

public class DGraphAcidTest extends AcidTest<DGraphDriver> {

    public DGraphAcidTest() {
        super(new DGraphDriver());
    }

}
