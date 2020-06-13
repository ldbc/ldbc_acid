package driver;

import java.util.Map;

public abstract class TestDriver<TestTransaction, QueryParameters, QueryResult> implements AutoCloseable {

    public abstract TestTransaction startTransaction();

    public abstract void commitTransaction(TestTransaction tt);

    public abstract void abortTransaction(TestTransaction tt);

    public abstract QueryResult runQuery(TestTransaction tt, String querySpecification, QueryParameters queryParameters);

    public void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public abstract void nukeDatabase();

    // LU

    public abstract void luInit();

    public abstract Void lu1();

    public abstract long lu2();

    // IMP

    public abstract void impInit();

    public abstract Map<String, Long> imp1(Map<String, Object> parameters);

    public abstract Map<String, Long> imp2(Map<String, Object> parameters);

    // PMP

    public abstract void pmpInit();

    public abstract Map<String, Long> pmp1(Map<String, Object> parameters);

    public abstract Map<String, Long> pmp2(Map<String, Object> parameters);


}
