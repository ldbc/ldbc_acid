package main;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class TransactionThread implements Callable<Void> {

    final Consumer<Void> f;

    public TransactionThread(Consumer<Void> f) {
        this.f = f;
    }

    @Override
    public Void call() throws Exception {
        f.accept(null);
        return null;
    }

}
