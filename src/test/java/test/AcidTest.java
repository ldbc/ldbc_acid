package test;

import com.google.common.collect.ImmutableMap;
import driver.TestDriver;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import transactions.TransactionThread;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public abstract class AcidTest<TTestDriver extends TestDriver> {

    protected TTestDriver testDriver;
    protected ExecutorService executorService = Executors.newFixedThreadPool(8);
    private boolean printStackTrace = false;

    public AcidTest(TTestDriver testDriver) {
        this.testDriver = testDriver;
    }

    @Before
    public void initialize() {
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        testDriver.nukeDatabase();
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void atomicityCTest() throws Exception {
        testDriver.atomicityInit();

        testDriver.atomicityC(ImmutableMap.of("person1Id", 1L, "person2Id", 3L, "newEmail", "alice@otherdomain.net", "since", 2020));

        final Map<String, Object> results = testDriver.atomicityCheck();
        Assert.assertEquals(3, (long) results.get("numPersons"));
        Assert.assertEquals(2, (long) results.get("numNames"));
        Assert.assertEquals(4, (long) results.get("numEmails"));
    }

    @Test
    public void atomicityRbTest() throws Exception {
        testDriver.atomicityInit();

        testDriver.atomicityRB(ImmutableMap.of("person1Id", 1L, "person2Id", 2L, "newEmail", "alice@otherdomain.net", "since", 2020));

        final Map<String, Object> results = testDriver.atomicityCheck();
        Assert.assertEquals(2, (long) results.get("numPersons"));
        Assert.assertEquals(2, (long) results.get("numNames"));
        Assert.assertEquals(3, (long) results.get("numEmails"));
    }

    @Test
    public void luTest() throws Exception {
        testDriver.luInit();
        final int nTransactions = 200;
        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 0; i < nTransactions; i++) {
            clients.add(new TransactionThread<>(i, testDriver::lu1, ImmutableMap.of("person1Id", 1L,"person2Id",(i+2L))));
        }
        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        int aborted = 0;
        for (Future<Map<String, Object>> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                aborted++;
                if(printStackTrace)
                    e.printStackTrace();
            }
        }
        System.out.printf("Number of aborted transactions: %d\n", aborted);

        Map<String, Object> results = testDriver.lu2(ImmutableMap.of("personId", 1L));
        final long numFriendsProp = (long) results.get("numFriendsProp");
        final long numKnowsEdges = (long) results.get("numKnowsEdges");
        final boolean pass = ((nTransactions-aborted == numFriendsProp) && (nTransactions-aborted == numKnowsEdges));
        System.out.printf("LU:    %4d %4d %4d %5b\n", nTransactions-aborted, numFriendsProp, numKnowsEdges, pass);

        Assert.assertEquals(nTransactions-aborted, numFriendsProp);
        Assert.assertEquals(nTransactions-aborted, numKnowsEdges);
    }

    @Test
    public void g0Test() throws Exception {
        testDriver.g0Init();
        final int wc = 200;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 1; i <= wc; i++) {
            clients.add(new TransactionThread<>(i, testDriver::g0, ImmutableMap.of("person1Id", 1L, "person2Id", 2L, "transactionId", i)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        int aborted = 0;
        for (Future<Map<String, Object>> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                aborted++;
                if(printStackTrace)e.printStackTrace();
            }
        }
        System.out.printf("Number of aborted transactions: %d\n", aborted);


        Map<String, Object> results = testDriver.g0check(ImmutableMap.of("person1Id", 1L, "person2Id", 2L));
        if (results.containsKey("p1VersionHistory")) {
            final List<Long> p1VersionHistory = new ArrayList<>((List<Long>) results.get("p1VersionHistory"));
            final List<Long> kVersionHistory  = new ArrayList<>((List<Long>) results.get("kVersionHistory" ));
            final List<Long> p2VersionHistory = new ArrayList<>((List<Long>) results.get("p2VersionHistory"));

            p1VersionHistory.retainAll(kVersionHistory);
            p1VersionHistory.retainAll(p2VersionHistory);

            kVersionHistory.retainAll(p1VersionHistory);
            kVersionHistory.retainAll(p2VersionHistory);

            p2VersionHistory.retainAll(p1VersionHistory);
            p2VersionHistory.retainAll(kVersionHistory);

            System.out.printf("G0:    %s %s %s %5b %5b %5b\n",
                    p1VersionHistory, kVersionHistory, p2VersionHistory,
                    p1VersionHistory.equals(kVersionHistory),
                    p1VersionHistory.equals(p2VersionHistory),
                    kVersionHistory.equals(p1VersionHistory));
            Assert.assertEquals(p1VersionHistory, kVersionHistory);
            Assert.assertEquals(p1VersionHistory, p2VersionHistory);
            Assert.assertEquals(kVersionHistory, p1VersionHistory);
        }
    }

    @Test
    public void g1aTest() throws Exception {
        testDriver.g1aInit();
        final int wc = 5;
        final int rc = 5;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 0; i < wc; i++) {
            clients.add(new TransactionThread<>(i, testDriver::g1a1, ImmutableMap.of("personId", 1L, "sleepTime", 250L)));
        }
        for (int i = 0; i < rc; i++) {
            clients.add(new TransactionThread<>(i, testDriver::g1a2, ImmutableMap.of("personId", 1L)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        int aborted = 0;
        for (Future<Map<String, Object>> future : futures) {
            try {
                final Map<String, Object> results = future.get();
                if (results.containsKey("pVersion")) {
                    final long pVersion = (long) results.get("pVersion");

                    System.out.printf("G1a:   %4d %4d %5b\n", 1L, pVersion, 1L == pVersion);
                    Assert.assertEquals(1L, pVersion);
                }
            } catch (Exception e) {
                aborted++;
                if(printStackTrace)e.printStackTrace();
            }
        }
        System.out.printf("Number of aborted transactions: %d\n", aborted);

    }

    @Test
    public void g1bTest() throws Exception {
        testDriver.g1bInit();
        final int wc = 10;
        final int rc = 100;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 0; i < wc; i++) {
            clients.add(new TransactionThread<>(i, testDriver::g1b1, ImmutableMap.of("personId", 1L, "even", 0L, "odd", 1L, "sleepTime", 1L)));
        }
        for (int i = 0; i < rc; i++) {
            clients.add(new TransactionThread<>(i, testDriver::g1b2, ImmutableMap.of("personId", 1L)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        for (Future<Map<String, Object>> future : futures) {
            final Map<String, Object> results = future.get();
            if (results.containsKey("pVersion")) {
                final long pVersion = (long) results.get("pVersion");

                System.out.printf("G1b:   %4d %4d %5b\n", 1L, pVersion, pVersion % 2 == 1);
                Assert.assertTrue(pVersion % 2 == 1);
            }
        }
    }

    @Test
    public void g1cTest() throws Exception {
        testDriver.g1cInit();
        final int c = 100;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        final Random random = new Random();
        for (long i = 1; i <= c; i++) {
            final boolean order = random.nextBoolean();
            long person1Id = order ? 1L : 2L;
            long person2Id = order ? 2L : 1L;
            clients.add(new TransactionThread<>(i, testDriver::g1c, ImmutableMap.of("person1Id", person1Id, "person2Id", person2Id, "transactionId", i)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        List<Map<String, Object>> resultss = new ArrayList<>();

        // ugly hack: we put NULLs in the list
        // TODO: use List<Optional<Map<String, Object>>>
        int aborted = 0;
        for (Future<Map<String, Object>> future : futures) {
            try {
                resultss.add(future.get());
            } catch (Exception e) {
                resultss.add(null);
                aborted++;
                if(printStackTrace)e.printStackTrace();
            }
        }
        System.out.printf("Number of aborted transactions: %d\n", aborted);

        // not that transactions are indexed from 1
        // but lists are indexed from 0
        for (int i = 1; i <= c; i++) {
            Map<String, Object> results1 = resultss.get(i-1);
            if (results1 == null) continue;

            final int person2Version1 = ((Long) results1.get("person2Version")).intValue();
            if (person2Version1 == 0L) continue;

            final Map<String, Object> results2 = resultss.get(person2Version1-1);
            Assert.assertNotNull(String.format("Transaction %d read data by aborted transaction %d", i, person2Version1), results2);

            final int person2Version2 = ((Long) results2.get("person2Version")).intValue();

            System.out.printf("G1c:   %4d %4d %4d %5b\n", i, person2Version1, person2Version2, i != person2Version2);
            Assert.assertNotEquals(i, person2Version2);
        }
    }

    @Test
    public void impTest() throws Exception {
        testDriver.impInit();
        final int wc = 10;
        final int rc = 10;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 0; i < wc; i++) {
            clients.add(new TransactionThread<>(i, testDriver::imp1, ImmutableMap.of("personId", 1L)));
        }
        for (int i = 0; i < rc; i++) {
            clients.add(new TransactionThread<>(i, testDriver::imp2, ImmutableMap.of("personId", 1L, "sleepTime", 250L)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);

        int aborted = 0;
        for (Future<Map<String, Object>> future : futures) {
            try {
                final Map<String, Object> results = future.get();
                if (results.containsKey("firstRead")) {
                    final long firstRead = (long) results.get("firstRead");
                    final long secondRead = (long) results.get("secondRead");
                    System.out.printf("IMP:   %4d %4d %5b\n", firstRead, secondRead, firstRead == secondRead);
                    Assert.assertEquals(firstRead, secondRead);
                }
            } catch (Exception e) {
                aborted++;
                if(printStackTrace)e.printStackTrace();
            }
        }
        System.out.printf("Number of aborted transactions: %d\n", aborted);
    }

    @Test
    public void pmpTest() throws Exception {
        testDriver.pmpInit();
        final int wc = 10;
        final int rc = 10;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 0; i < wc; i++) {
            clients.add(new TransactionThread<>(i, testDriver::pmp1, ImmutableMap.of("personId", 1L, "postId", 1L)));
        }
        for (int i = 0; i < rc; i++) {
            clients.add(new TransactionThread<>(i, testDriver::pmp2, ImmutableMap.of("personId", 1L, "postId", 1L, "sleepTime", 250L)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        for (Future<Map<String, Object>> future : futures) {
            final Map<String, Object> results = future.get();
            if (results.containsKey("firstRead")) {
                final long firstRead = (long) results.get("firstRead");
                final long secondRead = (long) results.get("secondRead");
                System.out.printf("PMP:   %4d %4d %5b\n", firstRead, secondRead, firstRead == secondRead);
                Assert.assertEquals(firstRead, secondRead);
            }
        }
    }

    @Test
    public void frTest() throws Exception {
        testDriver.frInit();
        final int wc = 1;
        final int rc = 100;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        for (int i = 0; i < wc; i++) {
            clients.add(new TransactionThread<>(i, testDriver::fr1, ImmutableMap.of("personId", 1L)));
        }
        for (int i = 0; i < rc; i++) {
            clients.add(new TransactionThread<>(i, testDriver::fr2, ImmutableMap.of("personId", 1L, "sleepTime", 250L)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        int aborted = 0;
        for (Future<Map<String, Object>> future : futures) {
            try {
                final Map<String, Object> results = future.get();
                if (results.containsKey("firstRead")) {
                    final List<Long> firstRead  = ((List<Long>) results.get("firstRead"));
                    final List<Long> secondRead = ((List<Long>) results.get("secondRead"));
                    System.out.printf("FR:   %4s %4s %5b\n", firstRead, secondRead, firstRead.equals(secondRead));
                    Assert.assertEquals(firstRead, secondRead);
                }
            } catch (Exception e) {
                aborted++;
                if(printStackTrace)e.printStackTrace();
            }
        }
        System.out.printf("Number of aborted transactions: %d\n", aborted);

    }

    @Test
    public void otvTest() throws Exception {
        testDriver.otvInit();
        final int rc = 50;

        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        clients.add(new TransactionThread<>(0, testDriver::otv1, ImmutableMap.of("cycleSize", 4)));
        Random random = new Random();
        for (int i = 0; i < rc; i++) {
            clients.add(new TransactionThread<>(i, testDriver::otv2, ImmutableMap.of("personId", random.nextInt(4)+1, "sleepTime", 250L)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        int aborted = 0;
        for (Future<Map<String, Object>> future : futures) {
            try {
                final Map<String, Object> results = future.get();
                if (results.containsKey("firstRead")) {
                    final List<Long> firstRead  = ((List<Long>) results.get("firstRead" ));
                    final List<Long> secondRead = ((List<Long>) results.get("secondRead"));
                    System.out.printf("OTV:   %4s %4s %5b\n", firstRead, secondRead, Collections.max(firstRead) <= Collections.min(secondRead));

                    Assert.assertTrue(Collections.max(firstRead) <= Collections.min(secondRead));
                }
            } catch (Exception e) {
                aborted++;
                if(printStackTrace)e.printStackTrace();
            }
        }
        System.out.printf("Number of aborted transactions: %d\n", aborted);



    }

    @Test
    public void wsTest() throws Exception {
        testDriver.wsInit();
        final int wc = 50;

        int numPersonPairs = 10;
        List<TransactionThread<Map<String, Object>, Map<String, Object>>> clients = new ArrayList<>();
        Random random = new Random();

        for (int i = 0; i < wc; i++) {
            // person1 indices range from 1 to 2*numPersonPairs+1
            long person1Id = random.nextInt(numPersonPairs)*2+1;
            long person2Id = person1Id + 1;
            clients.add(new TransactionThread<>(i, testDriver::ws1,
                    ImmutableMap.of("person1Id", person1Id, "person2Id", person2Id, "sleepTime", 250L)));
        }

        final List<Future<Map<String, Object>>> futures = executorService.invokeAll(clients);
        int aborted = 0;
        for (Future<Map<String, Object>> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                aborted++;
                if(printStackTrace)e.printStackTrace();
            }
        }
        System.out.printf("Number of aborted transactions: %d\n", aborted);

        Map<String, Object> results = testDriver.ws2(ImmutableMap.of());
        System.out.println(results);

        Assert.assertTrue(results.isEmpty());
    }

    @After
    public void cleanup() throws InterruptedException {
//        System.out.println(Thread.currentThread().getName() + ": Shutting down executor service...");
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.HOURS);
    }

}
