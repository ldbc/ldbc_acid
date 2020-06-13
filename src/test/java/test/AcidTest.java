package test;

import com.google.common.collect.ImmutableMap;
import driver.TestDriver;
import main.TransactionThread;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public abstract class AcidTest<TTestDriver extends TestDriver> {

    protected TTestDriver testDriver;
    protected ExecutorService executorService = Executors.newFixedThreadPool(8);

    public AcidTest(TTestDriver testDriver) {
        this.testDriver = testDriver;
    }

    @Before
    public void initialize() {
        testDriver.nukeDatabase();
    }

    @Test
    public void luTest() throws Exception {
        testDriver.luInit();
        final int nTransactions = 200;
        final List<TransactionThread<Void, Void>> clients = Collections.nCopies(nTransactions, new TransactionThread(x -> testDriver.lu1(), null));
        executorService.invokeAll(clients);

        final long nResults = testDriver.lu2();
        System.out.printf("LU:    %4d %4d %5b\n", nTransactions, nResults, nTransactions == nResults);
    }

    @Test
    public void impTest() throws Exception {
        testDriver.impInit();
        final int uc = 1;
        final int rc = 1;

        List<TransactionThread<Map<String, Object>, Map<String, Long>>> clients = new ArrayList<>();
        for (int i = 0; i < uc; i++) {
            clients.add(new TransactionThread<>(testDriver::imp1, ImmutableMap.of("personId", 1L)));
        }
        for (int i = 0; i < rc; i++) {
            clients.add(new TransactionThread<>(testDriver::imp2, ImmutableMap.of("personId", 1L, "sleepTime", 250L)));
        }

        final List<Future<Map<String, Long>>> futures = executorService.invokeAll(clients);
        for (Future<Map<String, Long>> future : futures) {
            final Map<String, Long> results = future.get();
            if (results.containsKey("firstRead")) {
                final long firstRead = results.get("firstRead").longValue();
                final long secondRead = results.get("secondRead").longValue();
                System.out.printf("IMP:   %4d %4d %5b\n", firstRead, secondRead, firstRead == secondRead);
            }
        }
    }

    @Test
    public void pmpTest() throws Exception {
        testDriver.pmpInit();
        final int uc = 1;
        final int rc = 1;

        List<TransactionThread<Map<String, Object>, Map<String, Long>>> clients = new ArrayList<>();
        for (int i = 0; i < uc; i++) {
            clients.add(new TransactionThread<>(testDriver::pmp1, ImmutableMap.of("personId", 1L, "postId", 1L)));
        }
        for (int i = 0; i < rc; i++) {
            clients.add(new TransactionThread<>(testDriver::pmp2, ImmutableMap.of("personId", 1L, "postId", 1L, "sleepTime", 250L)));
        }

        final List<Future<Map<String, Long>>> futures = executorService.invokeAll(clients);
        for (Future<Map<String, Long>> future : futures) {
            final Map<String, Long> results = future.get();
            if (results.containsKey("firstRead")) {
                final long firstRead = results.get("firstRead").longValue();
                final long secondRead = results.get("secondRead").longValue();
                System.out.printf("PMP:   %4d %4d %5b\n", firstRead, secondRead, firstRead == secondRead);
            }
        }
    }

    @After
    public void cleanup() throws InterruptedException {
//        System.out.println(Thread.currentThread().getName() + ": Shutting down executor service...");
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.HOURS);
    }

}
