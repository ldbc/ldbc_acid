package test;

import dgraph.DgraphDriver;

public class DgraphAcidTest extends AcidTest<DgraphDriver> {

    public DgraphAcidTest() {
        super(new DgraphDriver());
    }

}
