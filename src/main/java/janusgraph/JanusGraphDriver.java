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

    //****** IMP BLOCK ******//

    @Override
    public void impInit() {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource gWrite = transaction.traversal();
        Vertex v = gWrite.addV().next();
        v.property("id",1L);
        v.property("version",1L);
        transaction.commit();
    }

    @Override
    public Map<String, Object> imp1(Map parameters) {

        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource gWrite = transaction.traversal();
        long personID = (long) parameters.get("personId");

        if(gWrite.V().has("id",personID).hasNext()) {
            Vertex currentVertex = gWrite.V().has("id", personID).next();
            int currentVersion = currentVertex.value("version");
            currentVertex.property("version", currentVersion + 1L);
            commitTransaction(transaction);
            return ImmutableMap.of();
        }
        else
            throw new IllegalStateException("IMP1 Person Missing from Graph");

    }

    @Override
    public Map<String, Object> imp2(Map parameters) {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource gWrite = transaction.traversal();
        long personID  = (long) parameters.get("personId");
        long sleepTime = (long) parameters.get("sleepTime");
        int firstRead =  gWrite.V().has("id",personID).next().value("version");
        sleep(sleepTime);
        int secondRead = gWrite.V().has("id",personID).next().value("version");

        return ImmutableMap.of("firstRead", (long) firstRead, "secondRead", (long) secondRead);
    }

    //****** G1A BLOCK ******//

    @Override
    public void g1aInit() {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource gWrite = transaction.traversal();
        Vertex v = gWrite.addV().next();
        v.property("id",1L);
        v.property("version",1L);
        transaction.commit();
    }

    @Override
    public Map<String, Object> g1a1(Map parameters) {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource gWrite = transaction.traversal();
        long personID = (long) parameters.get("personId");
        long sleepTime = (long) parameters.get("sleepTime");
        if(gWrite.V().has("id",personID).hasNext()) {
            Vertex currentVertex = gWrite.V().has("id", personID).next();
            int currentVersion = currentVertex.value("version");
            sleep(sleepTime);
            currentVertex.property("version", currentVersion + 1L);
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
        GraphTraversalSource gWrite = transaction.traversal();
        long personID = (long) parameters.get("personId");
        if(gWrite.V().has("id",personID).hasNext()) {
            Vertex currentVertex = gWrite.V().has("id", personID).next();
            int pVersion = currentVertex.value("version");
            return ImmutableMap.of("pVersion", (long) pVersion);
        }
        else
            throw new IllegalStateException("G1A2 Person Missing from Graph");
    }


    //****** G1B BLOCK ******//

    @Override
    public void g1bInit() {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource gWrite = transaction.traversal();
        Vertex v = gWrite.addV().next();
        v.property("id",1L);
        v.property("version",99L);
        transaction.commit();
    }

    @Override
    public Map<String, Object> g1b1(Map parameters) {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource gWrite = transaction.traversal();
        long personID  = (long) parameters.get("personId");
        long sleepTime = (long) parameters.get("sleepTime");
        long even      = (long) parameters.get("even");
        long odd       = (long) parameters.get("odd");

        if(gWrite.V().has("id",personID).hasNext()) {
            Vertex currentVertex = gWrite.V().has("id", personID).next();
            currentVertex.property("version", even);
            sleep(sleepTime);
            currentVertex.property("version", odd);
            abortTransaction(transaction);
            return ImmutableMap.of();
        }
        else
            throw new IllegalStateException("G1A1 Person Missing from Graph");
    }

    @Override
    public Map<String, Object> g1b2(Map parameters) {
        JanusGraphTransaction transaction = startTransaction();
        GraphTraversalSource gWrite = transaction.traversal();
        long personID = (long) parameters.get("personId");
        if(gWrite.V().has("id",personID).hasNext()) {
            Vertex currentVertex = gWrite.V().has("id", personID).next();
            int pVersion = currentVertex.value("version");
            return ImmutableMap.of("pVersion", (long) pVersion);
        }
        else
            throw new IllegalStateException("G2A2 Person Missing from Graph");
    }




    @Override
    public void g0Init() {

    }





    @Override
    public void g1cInit() {

    }

    @Override
    public Map<String, Object> g1c(Map parameters) {
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
