package neo4j;

import com.google.common.collect.ImmutableMap;
import driver.TestDriver;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;

import java.util.Map;

public class Neo4jTestDriver extends TestDriver<Transaction, Map<String, Object>, Result> {

    protected Driver driver;

    public Neo4jTestDriver() {
        driver = GraphDatabase.driver("bolt://localhost:" + 7777, AuthTokens.basic("neo4j", "neo4j"));
    }

    @Override
    public void close() throws Exception {
        driver.close();
    }

    @Override
    public Transaction startTransaction() {
        return driver.session().beginTransaction();
    }

    @Override
    public void commitTransaction(Transaction tt) {
        tt.commit();
    }

    @Override
    public void abortTransaction(Transaction tt) {
        tt.rollback();
    }

    @Override
    public Result runQuery(Transaction tt, String querySpecification, Map<String, Object> queryParameters) {
        return tt.run(querySpecification, queryParameters);
    }

    @Override
    public void luInit() {
        final Transaction tt = startTransaction();
        tt.run("CREATE (:Person {id: 1, numFriends: 0})");
        tt.commit();
    }

    @Override
    public void lu1(Void aVoid) {
        final Transaction tt = startTransaction();
        tt.run("MATCH (p1:Person {id: 1})\n" +
                "SET p1.numFriends = p1.numFriends + 1\n" +
                "RETURN p1.numFriends\n");
        tt.commit();
    }

    @Override
    public long lu2() {
        final Transaction tt = startTransaction();
        final Result result = tt.run("MATCH (p:Person {id: 1})\n" +
                "OPTIONAL MATCH (p)-[k:KNOWS]->()\n" +
                "WITH p, count(k) AS numKnowsEdges\n" +
                "RETURN numKnowsEdges,\n" +
                "       p.numFriends AS numFriendsProp\n");
        final Record record = result.next();
        long numKnowsEdges = record.get("numKnowsEdges").asLong();
        long numFriends = record.get("numFriendsProp").asLong();
        return numFriends;
    }

    @Override
    public void impInit() {
        final Transaction tt = startTransaction();
        tt.run("CREATE (:Person {id: 0, version: 1}), (:Person {id: 1, version: 2})");
        tt.commit();
    }

    @Override
    public void imp1(long personId) {
        final Transaction tt = startTransaction();
        tt.run("MATCH (p:Person {id: $personId}) SET p.version = p.version + 1",
                ImmutableMap.of("personId", personId));
        tt.commit();
    }

    @Override
    public boolean imp2(long personId) {
        final Transaction tt = startTransaction();
        final Result result1 = tt.run("MATCH (p:Person {id: $personId}) RETURN p.version AS firstRead", ImmutableMap.of("personId", personId));
        if (!result1.hasNext()) {
            System.out.println("result1 empty");
            return false;
        }
        final long firstRead = result1.next().get("firstRead").asLong();

        try {
            Thread.sleep(250);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        final Result result2 = tt.run("MATCH (p:Person {id: $personId}) RETURN p.version AS secondRead", ImmutableMap.of("personId", personId));
        if (!result2.hasNext()) {
            System.out.println("result2 empty");
            return false;
        }
        final long secondRead = result2.next().get("secondRead").asLong();
        System.out.println(firstRead == secondRead);
        return firstRead == secondRead;
    }

}
