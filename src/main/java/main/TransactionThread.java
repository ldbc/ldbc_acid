package main;

import java.util.concurrent.Callable;
import java.util.function.Function;

public class TransactionThread<T, R> implements Callable<R> {

    final Function<T, R> f;
    final T t;

    public TransactionThread(Function<T, R> f, T t) {
        this.f = f;
        this.t = t;
    }

    @Override
    public R call() throws Exception {
        return f.apply(t);
    }

}
