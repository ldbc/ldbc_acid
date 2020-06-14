package test;


import janusgraph.JanusGraphDriver;

public class JanusGraphAcidTest extends AcidTest<JanusGraphDriver> {
    public JanusGraphAcidTest() {
        super(new JanusGraphDriver());
    }
}
