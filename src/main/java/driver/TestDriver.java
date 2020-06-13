package driver;

public abstract class TestDriver<TestTransaction, QueryParameters, QueryResult> implements AutoCloseable {

    public abstract TestTransaction startTransaction();

    public abstract void commitTransaction(TestTransaction tt);

    public abstract void abortTransaction(TestTransaction tt);

    public abstract QueryResult runQuery(TestTransaction tt, String querySpecification, QueryParameters queryParameters);

    public abstract void luInit();

    public abstract void lu1(Void aVoid);

    public abstract long lu2();

    public abstract void impInit();

    public abstract void imp1(long personId);

    public abstract boolean imp2(long personId);

}
