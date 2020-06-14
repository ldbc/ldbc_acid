package transactions;

import java.util.concurrent.Callable;
import java.util.function.Function;

public class TransactionThread<T, R> implements Callable<R> {

    final long transactionId;
    final Function<T, R> f;
    final T t;

    public TransactionThread(long transactionId, Function<T, R> f, T t) {
        this.transactionId = transactionId;
        this.f = f;
        this.t = t;
    }

    @Override
    public R call() throws Exception {
        return f.apply(t);
    }

    public long getTransactionId() {
        return transactionId;
    }

}
