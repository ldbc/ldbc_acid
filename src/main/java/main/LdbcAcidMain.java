package main;

import com.google.common.collect.ImmutableMap;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import utils.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class LdbcAcidMain {

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        int port = 7777;
        TestDatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder()
                .setConfig( BoltConnector.enabled, true )
                .setConfig( BoltConnector.listen_address, new SocketAddress( "localhost", port) );
        DatabaseManagementService managementService = builder.impermanent().build();

        //try (
        Driver driver = GraphDatabase.driver("bolt://localhost:" + port, AuthTokens.basic("neo4j", "neo4j"));
        // {
            Runtime.getRuntime().addShutdownHook(new Thread(managementService::shutdown));

            ExecutorService executorService = Executors.newFixedThreadPool(10);

            String atomicityC = Utils.readFile("queries/atomicity-c.cypher");
            String atomicityRb = Utils.readFile("queries/atomicity-rb.cypher");

            final Map<String, Object> params1 = ImmutableMap.of("person1Id", 1L, "person2Id", 2L, "newEmail", "a@b.com", "creationDate", 2020);
            final Map<String, Object> params2 = ImmutableMap.of("person1Id", 1L, "person2Id", 2L, "newEmail", "a@b.com");

//            System.out.println(atomicityC);
//            System.out.println(atomicityRb);

            final Session session = driver.session();

//            final Transaction tx1 = session.beginTransaction();
//            tx1.run(atomicityC, params1);
//            tx1.commit();
//
//            final Transaction tx2 = session.beginTransaction();
//            tx2.run(atomicityRb, params1);
//            tx2.commit();

            String lu1q = Utils.readFile("queries/lu1.cypher");
            String lu2q = Utils.readFile("queries/lu2.cypher");

            // LU init
            final ImmutableMap<String, Object> luParam = ImmutableMap.of("personId", 1);
            System.out.println("LU");
            Transaction tx = session.beginTransaction();
            tx.run("CREATE (:Person {id: $personId, numFriends: 0})", luParam);
            tx.commit();

            // LU1
            List<MyTxn> txns = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                final MyTxn txn = new MyTxn(lu1q, luParam, driver);
                txns.add(txn);
            }

            final List<Future<Void>> futures = executorService.invokeAll(txns);
            for (Future<Void> future : futures) {
                future.get();
//                System.out.print(".");
            }
            System.out.println();

        // LU2
            final Transaction lu2t = session.beginTransaction();
            final Result result = lu2t.run(lu2q, luParam);
            if (!result.hasNext()) {
                System.out.println("no results");
                return;
            }
            final Record record = result.next();
            long numKnowsEdges = record.get("numKnowsEdges").asLong();
            long numFriends = record.get("numFriendsProp").asLong();
            System.out.println(String.format("numKnowsEdges: %d, numFriends: %d", numKnowsEdges, numFriends));

            System.out.println(Thread.currentThread().getName() + ": Shutting down executor service...");
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.HOURS);
            System.out.println(Thread.currentThread().getName() + ": Executor service shutdown.");
            System.out.println(Thread.currentThread().getName() + ": Closing database...");
            managementService.shutdown();
            System.out.println(Thread.currentThread().getName() + ": Database closed.");

//        } finally {
//            managementService.shutdown();
//            System.out.println("shutdown ran.");
//            System.exit(0);
//        }
        System.exit(0);
    }

    private static void registerShutdownHook(final DatabaseManagementService managementService) {
        Runtime.getRuntime().addShutdownHook(new Thread(managementService::shutdown));
    }

}
