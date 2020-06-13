package neo4j;

import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class Neo4jEmbeddedDb implements AutoCloseable {

    protected DatabaseManagementService managementService;

    public Neo4jEmbeddedDb() {
        int port = 7777;
        managementService = new TestDatabaseManagementServiceBuilder()
                .setConfig( BoltConnector.enabled, true )
                .setConfig( BoltConnector.listen_address, new SocketAddress( "localhost", port) )
                .impermanent()
                .build();
        registerShutdownHook(managementService);


    }

    private static void registerShutdownHook(final DatabaseManagementService managementService) {
        Runtime.getRuntime().addShutdownHook(new Thread(managementService::shutdown));
    }

    @Override
    public void close() throws Exception {
        System.out.println(Thread.currentThread().getName() + ": Executor service shutdown.");
        System.out.println(Thread.currentThread().getName() + ": Closing database...");
        managementService.shutdown();
        System.out.println(Thread.currentThread().getName() + ": Database closed.");
    }
}
