package test;

import neo4j.Neo4jEmbeddedDb;
import neo4j.Neo4jTestDriver;
import org.junit.BeforeClass;

import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class Neo4jAcidTest extends AcidTest<Neo4jTestDriver> {

    @BeforeClass
    public static void setUp() {
        Neo4jEmbeddedDb db = new Neo4jEmbeddedDb();
        Logger rootLogger = LogManager.getLogManager().getLogger("");
        rootLogger.setLevel(Level.SEVERE);
        for (Handler h : rootLogger.getHandlers())
            h.setLevel(Level.SEVERE);
    }

    public Neo4jAcidTest() {
        super(new Neo4jTestDriver());
    }

}
