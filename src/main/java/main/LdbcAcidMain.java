package main;

import apoc.ApocSettings;
import com.google.common.collect.ImmutableMap;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import utils.Utils;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class LdbcAcidMain {

    public static void main(String[] args) throws IOException, InterruptedException {

        int port = 7777;
        TestDatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder()
                .setConfig( BoltConnector.enabled, true )
                .setConfig( BoltConnector.listen_address, new SocketAddress( "localhost", port) );
        DatabaseManagementService managementService = builder.impermanent().build();



        try (Driver driver = GraphDatabase.driver("bolt://localhost:" + port, AuthTokens.basic("neo4j", "neo4j"))) {
            Runtime.getRuntime().addShutdownHook(new Thread(managementService::shutdown));
            GraphDatabaseService db = managementService.database("neo4j");



            ExecutorService executorService = Executors.newFixedThreadPool(10); // 10 threads executing tasks
//            Set<Callable<...>> terminals = new HashSet<>();

//            int txnPerTerminal = 10;
//            for (int i = 1; i < 10; i++) {
//                terminals.add(new Terminal(db, 1, i,txnPerTerminal));
//            }

            String atomicityC = Utils.readFile("queries/atomicity-c.cypher");
            String atomicityRb = Utils.readFile("queries/atomicity-rb.cypher");

            final Map<String, Object> params1 = ImmutableMap.of("person1Id", 1, "person2Id", 2, "newEmail", "a@b.com", "creationDate", 2020);
            final Map<String, Object> params2 = ImmutableMap.of("person1Id", 1, "person2Id", 2, "newEmail", "a@b.com");

//            System.out.println(atomicityC);
//            System.out.println(atomicityRb);

            final Session session = driver.session();
            final Transaction tx1 = session.beginTransaction();
            tx1.run(atomicityC, params1);
            tx1.commit();

            final Transaction tx2 = session.beginTransaction();
            tx2.run(atomicityRb, params1);
            tx2.commit();



            System.out.println(Thread.currentThread().getName() + ": Shutting down executor service...");
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.HOURS);
            System.out.println(Thread.currentThread().getName() + ": Executor service shutdown.");
            System.out.println(Thread.currentThread().getName() + ": Closing database...");
            managementService.shutdown();
            System.out.println(Thread.currentThread().getName() + ": Database closed.");

        } finally {
            managementService.shutdown();
            System.out.println("shutdown ran.");
        }
    }

    private static void registerShutdownHook(final DatabaseManagementService managementService) {
        Runtime.getRuntime().addShutdownHook(new Thread(managementService::shutdown));
    }

}
