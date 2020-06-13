package test;

import com.google.common.collect.ImmutableMap;
import driver.TestDriver;
import transactions.TransactionThread;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        final List<TransactionThread<Void, Void>> clients = Collections.nCopies(nTransactions, new TransactionThread<>(x -> testDriver.lu1(), null));
        executorService.invokeAll(clients);

        final long nResults = testDriver.lu2();
        System.out.printf("LU:    %4d %4d %5b\n", nTransactions, nResults, nTransactions == nResults);
    }

    @Test
    public void g1aTest() throws Exception {
        testDriver.g1aInit();
        final int uc = 5;
        final int rc = 5;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 0; i < uc; i++) {
            clients.add(new TransactionThread<>(testDriver::g1a1, ImmutableMap.of("personId", 1L, "sleepTime", 250L)));
        }
        for (int i = 0; i < rc; i++) {
            clients.add(new TransactionThread<>(testDriver::g1a2, ImmutableMap.of("personId", 1L)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        for (Future<Map<String, Object>> future : futures) {
            final Map<String, Object> results = future.get();
            if (results.containsKey("pVersion")) {
                final long pVersion = (long) results.get("pVersion");

                System.out.printf("G1a:   %4d %4d %5b\n", 1L, pVersion, 1L == pVersion);
            }
        }
    }

    @Test
    public void g1bTest() throws Exception {
        testDriver.g1bInit();
        final int uc = 1;
        final int rc = 100;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 0; i < uc; i++) {
            clients.add(new TransactionThread<>(testDriver::g1b1, ImmutableMap.of("personId", 1L, "even", 0L, "odd", 1L, "sleepTime", 1L)));
        }
        for (int i = 0; i < rc; i++) {
            clients.add(new TransactionThread<>(testDriver::g1b2, ImmutableMap.of("personId", 1L)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        for (Future<Map<String, Object>> future : futures) {
            final Map<String, Object> results = future.get();
            if (results.containsKey("pVersion")) {
                final long pVersion = (long) results.get("pVersion");

                System.out.printf("G1b:   %4d %4d %5b\n", 1L, pVersion, 1L == pVersion);
            }
        }
    }

    @Test
    public void impTest() throws Exception {
        testDriver.impInit();
        final int uc = 1;
        final int rc = 1;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 0; i < uc; i++) {
            clients.add(new TransactionThread<>(testDriver::imp1, ImmutableMap.of("personId", 1L)));
        }
        for (int i = 0; i < rc; i++) {
            clients.add(new TransactionThread<>(testDriver::imp2, ImmutableMap.of("personId", 1L, "sleepTime", 250L)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        for (Future<Map<String, Object>> future : futures) {
            final Map<String, Object> results = future.get();
            if (results.containsKey("firstRead")) {
                final long firstRead = (long) results.get("firstRead");
                final long secondRead = (long) results.get("secondRead");
                System.out.printf("IMP:   %4d %4d %5b\n", firstRead, secondRead, firstRead == secondRead);
            }
        }
    }

    @Test
    public void pmpTest() throws Exception {
        testDriver.pmpInit();
        final int uc = 1;
        final int rc = 1;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 0; i < uc; i++) {
            clients.add(new TransactionThread<>(testDriver::pmp1, ImmutableMap.of("personId", 1L, "postId", 1L)));
        }
        for (int i = 0; i < rc; i++) {
            clients.add(new TransactionThread<>(testDriver::pmp2, ImmutableMap.of("personId", 1L, "postId", 1L, "sleepTime", 250L)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        for (Future<Map<String, Object>> future : futures) {
            final Map<String, Object> results = future.get();
            if (results.containsKey("firstRead")) {
                final long firstRead = (long) results.get("firstRead");
                final long secondRead = (long) results.get("secondRead");
                System.out.printf("PMP:   %4d %4d %5b\n", firstRead, secondRead, firstRead == secondRead);
            }
        }
    }

    @Test
    public void frTest() throws Exception {
        testDriver.frInit();
        final int uc = 1;
        final int rc = 100;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 0; i < uc; i++) {
            clients.add(new TransactionThread<>(testDriver::fr1, ImmutableMap.of("personId", 1L)));
        }
        for (int i = 0; i < rc; i++) {
            clients.add(new TransactionThread<>(testDriver::fr2, ImmutableMap.of("personId", 1L, "sleepTime", 250L)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        for (Future<Map<String, Object>> future : futures) {
            final Map<String, Object> results = future.get();
            if (results.containsKey("firstRead")) {
                final Set<Long> firstRead  = new HashSet<>((List<Long>) results.get("firstRead"));
                final Set<Long> secondRead = new HashSet<>((List<Long>) results.get("secondRead"));
                System.out.printf("FR:   %4s %4s %5b\n", firstRead, secondRead, !firstRead.equals(secondRead));
            }
        }
    }

    @Test
    public void otvTest() throws Exception {
        testDriver.otvInit();
        final int rc = 50;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        clients.add(new TransactionThread<>(testDriver::otv1, ImmutableMap.of("personId", 1L)));
        for (int i = 0; i < rc; i++) {
            clients.add(new TransactionThread<>(testDriver::otv2, ImmutableMap.of("personId", 1L, "sleepTime", 250L)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        for (Future<Map<String, Object>> future : futures) {
            final Map<String, Object> results = future.get();
            if (results.containsKey("firstRead")) {
                final List<Long> firstRead  = ((List<Long>) results.get("firstRead" ));
                final List<Long> secondRead = ((List<Long>) results.get("secondRead"));
                System.out.printf("OTV:   %4s %4s %5b\n", firstRead, secondRead, Collections.max(firstRead) <= Collections.min(secondRead));
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
