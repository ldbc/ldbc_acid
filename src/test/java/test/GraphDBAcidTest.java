package test;

import graphdb.GraphDBDriver;

public class GraphDBAcidTest extends AcidTest<GraphDBDriver> {
	public GraphDBAcidTest() {
		super(new GraphDBDriver("http://localhost:7201/repositories/ldbc-snb-acid-test"));
	}
}
