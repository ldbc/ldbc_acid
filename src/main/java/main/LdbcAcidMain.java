package main;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.neo4j.configuration.GraphDatabaseSettings.*;

public class LdbcAcidMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        DatabaseManagementService dms = null;
        try {
        FileUtils.deleteRecursively(new File("work-db"));
        FileUtils.copyRecursively(new File("base-db"), new File("work-db"));
        dms = new DatabaseManagementServiceBuilder(new File("work-db")).build();
            final GraphDatabaseService db = dms.database(DEFAULT_DATABASE_NAME);
            registerShutdownHook(dms);

            ExecutorService executorService = Executors.newFixedThreadPool(10); // 10 threads executing tasks
//            Set<Callable<...>> terminals = new HashSet<>();

//            int txnPerTerminal = 10;
//            for (int i = 1; i < 10; i++) {
//                terminals.add(new Terminal(db, 1, i,txnPerTerminal));
//            }


            System.out.println(Thread.currentThread().getName() + ": Shutting down executor service...");
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.HOURS);
            System.out.println(Thread.currentThread().getName() + ": Executor service shutdown.");
            System.out.println(Thread.currentThread().getName() + ": Closing database...");
            dms.shutdown();
            System.out.println(Thread.currentThread().getName() + ": Database closed.");

        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (dms != null) {
                dms.shutdown();
            }
        }
    }

    private static void registerShutdownHook(final DatabaseManagementService managementService) {
        Runtime.getRuntime().addShutdownHook(new Thread(managementService::shutdown));
    }

}
