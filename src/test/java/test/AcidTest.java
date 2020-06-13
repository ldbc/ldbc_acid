package test;

import driver.TestDriver;
import main.TransactionThread;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public abstract class AcidTest<TTestDriver extends TestDriver> {

    protected TTestDriver testDriver;
    protected ExecutorService executorService;

    public AcidTest(TTestDriver testDriver) {
        this.testDriver = testDriver;
    }

    @Before
    public void initialize() {
        executorService = Executors.newFixedThreadPool(8);
    }

//    @Test
//    public void luTest() throws Exception {
//        testDriver.luInit();
//        final int nTransactions = 200;
//        final List<TransactionThread> clients = Collections.nCopies(nTransactions, new TransactionThread(testDriver::lu1));
//        final List<Future<Void>> futures = executorService.invokeAll(clients);
//        for (Future<Void> future : futures) {
//            future.get();
//        }
//        final long nResults = testDriver.lu2();
//        System.out.println(String.format("LU:    %4d %4d %5b", nTransactions, nResults, nTransactions == nResults));
//    }

    @Test
    public void impTest() throws Exception {
        testDriver.impInit();
        final int uc = 50;
        final int rc = 50;

        List<TransactionThread> updateClients = new ArrayList<>();
        List<TransactionThread> readClients = new ArrayList<>();

        for (int i = 0; i < uc; i++) {
            final long personId = i % 2;
            updateClients.add(new TransactionThread(x -> testDriver.imp1(personId)));
        }
        for (int i = 0; i < rc; i++) {
            final long personId = i % 2;
            readClients.add(new TransactionThread(x -> testDriver.imp2(personId)));
        }

        final List<Future<Void>> updateFutures = executorService.invokeAll(updateClients);
        final List<Future<Void>> readFutures   = executorService.invokeAll(readClients);

        for (Future<Void> future : updateFutures) future.get();
        for (Future<Void> future : readFutures)   future.get();

//        System.out.println(String.format("IMP:   %4d %4d %5b", nTransactions, nResults, nTransactions == nResults));
    }

    @After
    public void cleanup() throws InterruptedException {
        System.out.println(Thread.currentThread().getName() + ": Shutting down executor service...");
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.HOURS);
    }

}
