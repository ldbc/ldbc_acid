package janusgraph;

import com.google.common.collect.ImmutableMap;
import driver.TestDriver;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;

import java.util.Map;

public class JanusGraphDriver extends TestDriver {

    private JanusGraph graph = JanusGraphFactory.open("conf/janusgraph-cassandra-es-server.properties");

    @Override
    public JanusGraphTransaction startTransaction()  {
        return graph.newTransaction();
    }

    @Override
    public void commitTransaction(Object tt) {
        JanusGraphTransaction transaction = (JanusGraphTransaction) tt;
        transaction.commit();
    }

    @Override
    public void abortTransaction(Object tt) {
        JanusGraphTransaction transaction = (JanusGraphTransaction) tt;
        transaction.rollback();
    }

    @Override
    public void close()  {
        graph.close();
    }


    @Override
    public void nukeDatabase() {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource g = transaction.traversal();
        g.V().drop().iterate(); //drop all vertices
        commitTransaction(transaction);

        close();//restart connection
        graph = JanusGraphFactory.open("conf/janusgraph-cassandra-es-server.properties");

    }



    //****** G1A BLOCK ******//

    @Override
    public void g1aInit() {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource g = transaction.traversal();
        Vertex v = g.addV().next();
        v.property("id",1L);
        v.property("version",1L);
        transaction.commit();
    }

    @Override
    public Map<String, Object> g1a1(Map parameters) {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource g = transaction.traversal();
        long personID = (long) parameters.get("personId");
        long sleepTime = (long) parameters.get("sleepTime");
        if(g.V().has("id",personID).hasNext()) {
            Vertex person = g.V().has("id", personID).next();
            int currentVersion = person.value("version");
            sleep(sleepTime);
            person.property("version", currentVersion + 1L);
            sleep(sleepTime);
            abortTransaction(transaction);
            return ImmutableMap.of();
        }
        else
            throw new IllegalStateException("G1A1 Person Missing from Graph");
    }

    @Override
    public Map<String, Object> g1a2(Map parameters) {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource g = transaction.traversal();
        long personID = (long) parameters.get("personId");
        if(g.V().has("id",personID).hasNext()) {
            Vertex person = g.V().has("id", personID).next();
            int pVersion = person.value("version");
            return ImmutableMap.of("pVersion", (long) pVersion);
        }
        else
            throw new IllegalStateException("G1A2 Person Missing from Graph");
    }


    //****** G1B BLOCK ******//

    @Override
    public void g1bInit() {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource g = transaction.traversal();
        Vertex v = g.addV().next();
        v.property("id",1L);
        v.property("version",99L);
        transaction.commit();
    }

    @Override
    public Map<String, Object> g1b1(Map parameters) {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource g = transaction.traversal();
        long personID  = (long) parameters.get("personId");
        long sleepTime = (long) parameters.get("sleepTime");
        long even      = (long) parameters.get("even");
        long odd       = (long) parameters.get("odd");

        if(g.V().has("id",personID).hasNext()) {
            Vertex person = g.V().has("id", personID).next();
            person.property("version", even);
            sleep(sleepTime);
            person.property("version", odd);
            abortTransaction(transaction);
            return ImmutableMap.of();
        }
        else
            throw new IllegalStateException("G1B1 Person Missing from Graph");
    }

    @Override
    public Map<String, Object> g1b2(Map parameters) {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource g = transaction.traversal();
        long personID = (long) parameters.get("personId");
        if(g.V().has("id",personID).hasNext()) {
            Vertex person = g.V().has("id", personID).next();
            int pVersion = person.value("version");
            return ImmutableMap.of("pVersion", (long) pVersion);
        }
        else
            throw new IllegalStateException("G2B2 Person Missing from Graph");
    }

    //****** G1C BLOCK ******//
    @Override
    public void g1cInit() {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource g = transaction.traversal();
        Vertex v1 = g.addV().next();
        v1.property("id",1L);
        v1.property("version",0L);
        Vertex v2 = g.addV().next();
        v2.property("id",2L);
        v2.property("version",0L);
        transaction.commit();
    }


    @Override
    public Map<String, Object> g1c(Map parameters) {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource g = transaction.traversal();
        long person1ID      = (long) parameters.get("person1Id");
        long person2ID      = (long) parameters.get("person2Id");
        long transactionId  = (long) parameters.get("transactionId");

        if(g.V().has("id",person1ID).hasNext() &&
           g.V().has("id",person2ID).hasNext()) {
            Vertex person1 = g.V().has("id", person1ID).next();
            person1.property("version", transactionId);
            Vertex person2 = g.V().has("id", person2ID).next();
            int person2Version = person2.value("version");
            commitTransaction(transaction);
            return ImmutableMap.of("person2Version", person2Version);
        }
        else
            throw new IllegalStateException("G1C Person Missing from Graph");
    }


    //****** IMP BLOCK ******//

    @Override
    public void impInit() {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource g = transaction.traversal();
        Vertex v = g.addV().next();
        v.property("id",1L);
        v.property("version",1L);
        transaction.commit();
    }

    @Override
    public Map<String, Object> imp1(Map parameters) {

        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource g = transaction.traversal();
        long personID = (long) parameters.get("personId");

        if(g.V().has("id",personID).hasNext()) {
            Vertex person = g.V().has("id", personID).next();
            int currentVersion = person.value("version");
            person.property("version", currentVersion + 1L);
            commitTransaction(transaction);
            return ImmutableMap.of();
        }
        else
            throw new IllegalStateException("IMP1 Person Missing from Graph");

    }

    @Override
    public Map<String, Object> imp2(Map parameters) {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource g = transaction.traversal();
        long personID  = (long) parameters.get("personId");
        long sleepTime = (long) parameters.get("sleepTime");
        int firstRead =  g.V().has("id",personID).next().value("version");
        sleep(sleepTime);
        int secondRead = g.V().has("id",personID).next().value("version");

        return ImmutableMap.of("firstRead", (long) firstRead, "secondRead", (long) secondRead);
    }


    @Override
    public void g0Init() {

    }

    @Override
    public Map<String, Object> g0check(Map parameters) {
        return null;
    }

    @Override
    public void pmpInit() {

    }

    @Override
    public void otvInit() {

    }

    @Override
    public void frInit() {

    }

    @Override
    public void luInit() {

    }

    @Override
    public Void lu1() {
        return null;
    }

    @Override
    public long lu2() {
        return 0;
    }

    @Override
    public void wsInit() {

    }

    @Override
    public Map<String, Object> ws2(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> ws1(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> fr2(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> fr1(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> otv2(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> otv1(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> pmp2(Map parameters) {
        return null;
    }

    @Override
    public Map<String, Object> pmp1(Map parameters) {
        return null;
    }



    @Override
    public Map<String, Object> g0(Map parameters) {
        return null;
    }


    @Override
    public Object runQuery(Object tt, String querySpecification, Object o) throws Exception {
        return null;
    }

}
