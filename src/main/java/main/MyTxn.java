package main;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Transaction;

import java.util.Map;
import java.util.concurrent.Callable;

public class MyTxn implements Callable<Void> {

    final String query;
    final Map<String, Object> parameters;
    final Driver driver;

    public MyTxn(String query, Map<String, Object> parameters, Driver driver) {
        this.query = query;
        this.parameters = parameters;
        this.driver = driver;
    }

    @Override
    public Void call() throws Exception {
        final Transaction tx = driver.session().beginTransaction();
        tx.run(query, parameters);
        tx.commit();
        return null;
    }

}
