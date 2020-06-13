package neo4j;

import com.google.common.collect.ImmutableMap;
import driver.TestDriver;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;

import java.util.List;
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
    public void nukeDatabase() {
        final Transaction tt = startTransaction();
        tt.run("MATCH (n) DETACH DELETE n");
        tt.commit();
    }

    @Override
    public void g0Init() {

    }

    @Override
    public Map<String, Object> g0(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public void g1aInit() {

    }

    @Override
    public Map<String, Object> g1a1(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public Map<String, Object> g1a2(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public void g1bInit() {
        final Transaction tt = startTransaction();
        tt.run("CREATE (:Person {id: 1, version: 99})");
        tt.commit();
    }

    @Override
    public Map<String, Object> g1b1(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();

        tt.run("MATCH (p:Person {id: $personId}) SET p.version = $even", parameters);
        sleep((Long) parameters.get("sleepTime"));
        tt.run("MATCH (p:Person {id: $personId}) SET p.version = $odd", parameters);

        tt.commit();
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> g1b2(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();

        final Result result = tt.run("MATCH (p:Person {id: $personId}) RETURN p.version AS pVersion", parameters);
        if (!result.hasNext()) throw new IllegalStateException("G1b T2 result empty");
        final long pVersion = result.next().get("pVersion").asLong();

        return ImmutableMap.of("pVersion", pVersion);
    }

    @Override
    public void g1cInit() {
    }

    @Override
    public Map<String, Object> g1c1(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public Map<String, Object> g1c2(Map<String, Object> parameters) {
        return null;
    }

    // IMP

    @Override
    public void impInit() {
        final Transaction tt = startTransaction();
        tt.run("CREATE (:Person {id: 1, version: 1})");
        tt.commit();
    }

    @Override
    public Map<String, Object> imp1(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();
        tt.run("MATCH (p:Person {id: $personId}) SET p.version = p.version + 1 RETURN p", parameters);
        tt.commit();
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> imp2(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();

        final Result result1 = tt.run("MATCH (p:Person {id: $personId}) RETURN p.version AS firstRead", parameters);
        if (!result1.hasNext()) throw new IllegalStateException("IMP result1 empty");
        final long firstRead = result1.next().get("firstRead").asLong();

        sleep((Long) parameters.get("sleepTime"));

        final Result result2 = tt.run("MATCH (p:Person {id: $personId}) RETURN p.version AS secondRead", parameters);
        if (!result2.hasNext()) throw new IllegalStateException("IMP result2 empty");
        final long secondRead = result2.next().get("secondRead").asLong();

        return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
    }

    // PMP

    @Override
    public void pmpInit() {
        final Transaction tt = startTransaction();
        tt.run("CREATE (:Person {id: 1}), (:Post {id: 1})");
        tt.commit();
    }

    @Override
    public Map<String, Object> pmp1(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();
        tt.run("MATCH (pe:Person {id: $personId}), (po:Post {id: $postId}) CREATE (pe)-[:LIKES]->(po)", parameters);
        tt.commit();
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> pmp2(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();

        final Result result1 = tt.run("MATCH (po1:Post {id: $postId})<-[:LIKES]-(pe1:Person) RETURN count(pe1) AS firstRead", parameters);
        if (!result1.hasNext()) throw new IllegalStateException("PMP result1 empty");
        final long firstRead = result1.next().get("firstRead").asLong();

        sleep((Long) parameters.get("sleepTime"));

        final Result result2 = tt.run("MATCH (po2:Post {id: $postId})<-[:LIKES]-(pe2:Person) RETURN count(pe2) AS secondRead", parameters);
        if (!result2.hasNext()) throw new IllegalStateException("PMP result2 empty");
        final long secondRead = result2.next().get("secondRead").asLong();

        return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
    }

    @Override
    public void otvInit() {

    }

    @Override
    public Map<String, Object> otv(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public void frInit() {
        final Transaction tt = startTransaction();
        tt.run("CREATE (p1:Person {id: 1, version: 0})-[:KNOWS]->" +
                "            (  :Person {id: 2, version: 0})-[:KNOWS]->" +
                "            (  :Person {id: 3, version: 0})-[:KNOWS]->" +
                "            (  :Person {id: 4, version: 0})-[:KNOWS]->(p1)");
        tt.commit();
    }

    @Override
    public Map<String, Object> fr1(Map<String, Object> parameters) {

        final Transaction tt = startTransaction();
        tt.run("MATCH path = (n:Person {id: $personId})-[:KNOWS*..4]->(n)\n" +
                "        UNWIND nodes(path) AS person WITH DISTINCT person\n" +
                "        SET person.version = person.version + 1", parameters);
        tt.commit();

        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> fr2(Map<String, Object> parameters) {
        final Transaction tt = startTransaction();

        final Result result1 = tt.run("MATCH path1 = (n1:Person {id: $personId})-[:KNOWS*..4]->(n1) RETURN [p IN nodes(path1) | p.version] AS firstRead", parameters);
        if (!result1.hasNext()) throw new IllegalStateException("FR2 result1 empty");
        final List<Object> firstRead = result1.next().get("firstRead").asList();

        sleep((Long) parameters.get("sleepTime"));

        final Result result2 = tt.run("MATCH path1 = (n1:Person {id: $personId})-[:KNOWS*..4]->(n1) RETURN [p IN nodes(path1) | p.version] AS secondRead", parameters);
        if (!result2.hasNext()) throw new IllegalStateException("FR2 result2 empty");
        final List<Object> secondRead = result2.next().get("secondRead").asList();

        return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
    }

    // LU

    @Override
    public void luInit() {
        final Transaction tt = startTransaction();
        tt.run("CREATE (:Person {id: 1, numFriends: 0})");
        tt.commit();
    }

    @Override
    public Void lu1() {
        final Transaction tt = startTransaction();
        tt.run("MATCH (p1:Person {id: 1})\n" +
                "SET p1.numFriends = p1.numFriends + 1\n" +
                "RETURN p1.numFriends\n");
        tt.commit();
        return null;
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
    public void wsInit() {

    }

    @Override
    public Map<String, Object> ws1(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public Map<String, Object> ws2(Map<String, Object> parameters) {
        return null;
    }


}
