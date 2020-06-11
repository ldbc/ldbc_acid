package main;

import apoc.ApocSettings;
import com.google.common.collect.ImmutableMap;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import utils.Utils;

public class LdbcAcidMain {

    public static void main(String[] args) throws IOException, InterruptedException {

        TestDatabaseManagementServiceBuilder builder = new TestDatabaseManagementServiceBuilder();
        builder.setConfig(ApocSettings.apoc_import_file_enabled, true);
        builder.setConfig(GraphDatabaseSettings.load_csv_file_url_root, new File("csvs").toPath().toAbsolutePath());

        DatabaseManagementService managementService = builder.impermanent().build();
        try {
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

            System.out.println(atomicityC);
            db.executeTransactionally(atomicityC, params1);

            System.out.println(atomicityRb);
            db.executeTransactionally(atomicityRb, params2);

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
