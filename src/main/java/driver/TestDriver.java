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

    // G0 Dirty Write

    public abstract void g0Init();

    public abstract Map<String, Object> g0(Map<String, Object> parameters);


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

    public abstract Map<String, Object> g1c1(Map<String, Object> parameters);

    public abstract Map<String, Object> g1c2(Map<String, Object> parameters);

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

    public abstract Void lu1();

    public abstract long lu2();

    // WS

    public abstract void wsInit();

    public abstract Map<String, Object> ws1(Map<String, Object> parameters);

    public abstract Map<String, Object> ws2(Map<String, Object> parameters);


}
