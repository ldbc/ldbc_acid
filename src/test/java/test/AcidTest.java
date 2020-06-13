package test;

import driver.TestDriver;
import main.TransactionThread;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    @Test
    public void luTest() throws Exception {
        testDriver.luInit();
        final int nTransactions = 200;
        final List<TransactionThread> clients = Collections.nCopies(nTransactions, new TransactionThread(testDriver::lu1));
        executorService.invokeAll(clients);

        final long nResults = testDriver.lu2();
        System.out.println(String.format("LU:    %4d %4d %5b", nTransactions, nResults, nTransactions == nResults));
    }

    @Test
    public void impTest() throws Exception {
        testDriver.impInit();
        final int uc = 1;
        final int rc = 1;

        List<TransactionThread> clients = new ArrayList<>();

        for (int i = 0; i < uc; i++) {
            final long personId = 1;
            clients.add(new TransactionThread(x -> testDriver.imp1(personId)));
        }
        for (int i = 0; i < rc; i++) {
            final long personId = 1;
            clients.add(new TransactionThread(x -> testDriver.imp2(personId)));
        }

        executorService.invokeAll(clients);

//        System.out.println(String.format("IMP:   %4d %4d %5b", nTransactions, nResults, nTransactions == nResults));
    }

    @After
    public void cleanup() throws InterruptedException {
        System.out.println(Thread.currentThread().getName() + ": Shutting down executor service...");
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.HOURS);
    }

}
