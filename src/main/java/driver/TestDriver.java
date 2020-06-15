package driver;

import java.util.Map;

public abstract class TestDriver<TestTransaction, QueryParameters, QueryResult> implements AutoCloseable {

    public abstract TestTransaction startTransaction() throws Exception;

    public abstract void commitTransaction(TestTransaction tt) throws Exception;

    public abstract void abortTransaction(TestTransaction tt) throws Exception;

    public abstract QueryResult runQuery(TestTransaction tt, String querySpecification, QueryParameters queryParameters) throws Exception;

    public void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public abstract void nukeDatabase();

    // Atomicity tests

    public abstract void atomicityInit();

    public abstract void atomicityC(Map<String, Object> parameters);

    public abstract void atomicityRB(Map<String, Object> parameters);

    public abstract Map<String, Object> atomicityCheck();

    // G0 Dirty Write

    public abstract void g0Init();

    public abstract Map<String, Object> g0(Map<String, Object> parameters);

    public abstract Map<String, Object> g0check(Map<String, Object> parameters);

    // G1a Intermediate Reads

    public abstract void g1aInit();

    public abstract Map<String, Object> g1a1(Map<String, Object> parameters);

    public abstract Map<String, Object> g1a2(Map<String, Object> parameters);

    // G1b Intermediate Reads

    public abstract void g1bInit();

    public abstract Map<String, Object> g1b1(Map<String, Object> parameters);

    public abstract Map<String, Object> g1b2(Map<String, Object> parameters);

    // G1b Intermediate Reads

    public abstract void g1cInit();

    public abstract Map<String, Object> g1c(Map<String, Object> parameters);

    // IMP

    public abstract void impInit();

    public abstract Map<String, Object> imp1(Map<String, Object> parameters);

    public abstract Map<String, Object> imp2(Map<String, Object> parameters);

    // PMP

    public abstract void pmpInit();

    public abstract Map<String, Object> pmp1(Map<String, Object> parameters);

    public abstract Map<String, Object> pmp2(Map<String, Object> parameters);

    // OTV

    public abstract void otvInit();

    public abstract Map<String, Object> otv1(Map<String, Object> parameters);

    public abstract Map<String, Object> otv2(Map<String, Object> parameters);

    // FR

    public abstract void frInit();

    public abstract Map<String, Object> fr1(Map<String, Object> parameters);

    public abstract Map<String, Object> fr2(Map<String, Object> parameters);

    // LU

    public abstract void luInit();

    public abstract Map<String, Object> lu1(Map<String, Object> parameters);

    public abstract Map<String, Object> lu2(Map<String, Object> parameters);

    // WS

    public abstract void wsInit();

    public abstract Map<String, Object> ws1(Map<String, Object> parameters);

    public abstract Map<String, Object> ws2(Map<String, Object> parameters);


}
