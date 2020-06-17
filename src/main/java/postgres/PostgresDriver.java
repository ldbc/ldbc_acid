package postgres;

import com.google.common.collect.ImmutableMap;
import driver.TestDriver;
import org.postgresql.ds.PGConnectionPoolDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class PostgresDriver extends TestDriver<Connection, Map<String, Object>, ResultSet> {

    protected PGConnectionPoolDataSource ds;

    public PostgresDriver() {
    }

    public void initDataSource(String host, int port, String username, String password, String dbName) {
        ds = new PGConnectionPoolDataSource();
        ds.setDefaultAutoCommit(false);
        ds.setDatabaseName(dbName);
        ds.setServerName(host);
        ds.setPortNumber(port);
        ds.setUser(username);
        ds.setPassword(password);
    }

    public String getPGVersion() throws SQLException {
        Connection conn = ds.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select version()");
        rs.next();
        return rs.getString(1);
    }

    @Override
    public Connection startTransaction() throws SQLException {
        Connection conn = ds.getConnection();
        conn.setAutoCommit(false);

        //conn.createStatement().executeUpdate(PostgresQueries.isolation_serializable);
        //conn.createStatement().executeUpdate(PostgresQueries.isolation_repetable_read);
        conn.createStatement().executeUpdate(PostgresQueries.isolation_read_committed);

        return conn;
    }

    @Override
    public void commitTransaction(Connection tt) throws SQLException {
        tt.commit();
    }

    @Override
    public void abortTransaction(Connection tt) throws SQLException {
        tt.rollback(); // rollback VS. abort?
    }

    @Override
    public ResultSet runQuery(Connection tt, String querySpecification, Map<String, Object> stringStringMap) throws Exception {
        ResultSet rs = null;
        querySpecification = substituteParameters(querySpecification, stringStringMap);
        // don't close this statement before returning (e.g. in a try-with-resources block) so we can retrive elements of the resultset
        Statement st = tt.createStatement();
        rs = st.executeQuery(querySpecification);
        return rs;
    }

    protected List<Object> toObjectList(Object o) {
        Object[] ol = (Object[])o;
        List<Object> lo = new ArrayList<Object>();
        for(Object i: ol) {
            lo.add(i);
        }
        return lo;
    }

    public String substituteParameters(String querySpecification, Map<String, Object> stringStringMap) {
        // substitute parameters
        if (stringStringMap != null) {
            for (Map.Entry<String, Object> param : stringStringMap.entrySet()) {
                querySpecification = querySpecification.replace("$" + param.getKey(), param.getValue().toString());
            }
        }
        //System.err.println(querySpecification);
        return querySpecification;
    }

    protected void executeUpdates(String[] commands) {
        executeUpdates(commands, true);
    }

    protected void executeUpdates(String[] commands, boolean doCommit) {
        try (Connection conn = startTransaction(); Statement st = conn.createStatement()) {
            for (String sql : commands) {
                // we never need info on the resultset, if any
                // so we use execute (which allows INSERT/UPDATE/DELETE/SELECT) instead of executeUpdate (which allows only INSERT/UPDATE/DELETE)
                st.execute(sql);
            }
            if (doCommit) {
                commitTransaction(conn);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void executeUpdates(String[] commands, Map<String, Object> parameters, boolean doCommit) {
        for(int i=0; i<commands.length; i++) {
            commands[i]=substituteParameters(commands[i], parameters);
        }
        executeUpdates(commands, doCommit);
    }

    protected void executeUpdates(Connection conn, String[] commands) {
        executeUpdates(conn, commands, true);
    }

    protected void executeUpdates(Connection conn, String[] commands, boolean doCommit) {
        try (Statement st = conn.createStatement()) {
            for (String sql : commands) {
                // we never need info on the resultset, if any
                // so we use execute (which allows INSERT/UPDATE/DELETE/SELECT) instead of executeUpdate (which allows only INSERT/UPDATE/DELETE)
                st.execute(sql);
            }
            if (doCommit) {
                commitTransaction(conn);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void executeUpdates(Connection conn, String[] commands, Map<String, Object> parameters, boolean doCommit) {
        for(int i=0; i<commands.length; i++) {
            commands[i]=substituteParameters(commands[i], parameters);
        }
        executeUpdates(conn, commands, doCommit);
    }


    protected void createSchema() {
        executeUpdates(PostgresQueries.tablesCreate);
    }

    @Override
    public void nukeDatabase() {
        // drop all the tables
        executeUpdates(PostgresQueries.tablesClear);
    }

    @Override
    public void atomicityInit() {
        createSchema();
        executeUpdates(PostgresQueries.atomicityInit);
    }

    @Override
    public void atomicityC(Map<String, Object> parameters) {
        executeUpdates(PostgresQueries.atomicityCTx, parameters, true);
    }


    @Override
    public void atomicityRB(Map<String, Object> parameters) {
        try (Connection conn = startTransaction()) {
            executeUpdates(conn, PostgresQueries.atomicityRBxP1update, parameters, false);
            ResultSet rs = runQuery(conn, PostgresQueries.atomicityRBxP2check, parameters);
            if (rs.next()) {
                abortTransaction(conn);
            } else {
                executeUpdates(conn, PostgresQueries.atomicityRBxP2create, parameters, false);
                commitTransaction(conn);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Object> atomicityCheck() {
        try (Connection conn = startTransaction()) {
            ResultSet rs = runQuery(conn, PostgresQueries.atomicityCheck, null);
            rs.next();

            final long numPersons = rs.getLong(1);
            final long numNames = rs.getLong(2);
            final long numEmails = rs.getLong(3);

            return ImmutableMap.of("numPersons", numPersons, "numNames", numNames, "numEmails", numEmails);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void g0Init() {
        createSchema();
        executeUpdates(PostgresQueries.g0Init);
    }

    @Override
    public Map<String, Object> g0(Map<String, Object> parameters) {
        executeUpdates(PostgresQueries.g0, parameters, true);

        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> g0check(Map<String, Object> parameters) {
        try (Connection conn = startTransaction()) {
            ResultSet rs = runQuery(conn, PostgresQueries.g0check, parameters);
            rs.next();

            List<Object> p1VersionHistory = toObjectList((Object[])rs.getArray(1).getArray());
            List<Object> kVersionHistory  = toObjectList((Object[])rs.getArray(2).getArray());
            List<Object> p2VersionHistory = toObjectList((Object[])rs.getArray(3).getArray());

            return ImmutableMap.of("p1VersionHistory", p1VersionHistory, "kVersionHistory", kVersionHistory, "p2VersionHistory", p2VersionHistory);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void g1aInit() {
        createSchema();
        executeUpdates(PostgresQueries.g1aInit);
    }

    @Override
    public Map<String, Object> g1a1(Map<String, Object> parameters) {
        try (Connection conn = startTransaction()) {
            executeUpdates(conn, PostgresQueries.g1a1, parameters, false);
            abortTransaction(conn);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Object> g1a2(Map<String, Object> parameters) {
        try (Connection conn = startTransaction()) {
            final ResultSet result = runQuery(conn, PostgresQueries.g1a2, parameters);
            if (!result.next()) throw new IllegalStateException("G1a T2 result empty");
            final long pVersion = result.getLong(1);

            return ImmutableMap.of("pVersion", pVersion);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void g1bInit() {
        createSchema();
        executeUpdates(PostgresQueries.g1bInit);
    }

    @Override
    public Map<String, Object> g1b1(Map<String, Object> parameters) {
        try (Connection conn = startTransaction()) {
            executeUpdates(conn, PostgresQueries.g1b1, parameters, true);

            return ImmutableMap.of();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
     }

    @Override
    public Map<String, Object> g1b2(Map<String, Object> parameters) {
        try (Connection conn = startTransaction()) {
            final ResultSet result = runQuery(conn, PostgresQueries.g1b2, parameters);
            if (!result.next()) throw new IllegalStateException("G1b T2 result empty");
            final long pVersion = result.getLong(1);

            return ImmutableMap.of("pVersion", pVersion);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void g1cInit() {
        createSchema();
        executeUpdates(PostgresQueries.g1cInit);
    }

    @Override
    public Map<String, Object> g1c(Map<String, Object> parameters) {
        try (Connection conn = startTransaction()) {
            executeUpdates(conn, PostgresQueries.g1c1, parameters, false);
            final ResultSet result = runQuery(conn, PostgresQueries.g1c2, parameters);
            if (!result.next()) throw new IllegalStateException("G1c T2 result empty");
            final long person2Version = result.getLong(1);
            commitTransaction(conn);

            return ImmutableMap.of("person2Version", person2Version);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void impInit() {
        createSchema();
        executeUpdates(PostgresQueries.impInit);
    }

    @Override
    public Map<String, Object> imp1(Map<String, Object> parameters) {
        try (Connection conn = startTransaction()) {
            executeUpdates(conn, PostgresQueries.imp1, parameters, true);

            return ImmutableMap.of();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Object> imp2(Map<String, Object> parameters) {
        try (Connection conn = startTransaction()) {
            final ResultSet result1 = runQuery(conn, PostgresQueries.imp2, parameters);
            if (!result1.next()) throw new IllegalStateException("IMP result1 empty");
            final long firstRead = result1.getLong(1);

            sleep((Long) parameters.get("sleepTime"));

            final ResultSet result2 = runQuery(conn, PostgresQueries.imp2, parameters);
            if (!result2.next()) throw new IllegalStateException("IMP result2 empty");
            final long secondRead = result2.getLong(1);

            commitTransaction(conn);

            return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void pmpInit() {
        createSchema();
        executeUpdates(PostgresQueries.pmpInit);
    }

    @Override
    public Map<String, Object> pmp1(Map<String, Object> parameters) {
        try (Connection conn = startTransaction()) {
            executeUpdates(conn, PostgresQueries.pmp1, parameters, true);

            return ImmutableMap.of();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Object> pmp2(Map<String, Object> parameters) {
        try (Connection conn = startTransaction()) {
            final ResultSet result1 = runQuery(conn, PostgresQueries.pmp2, parameters);
            if (!result1.next()) throw new IllegalStateException("PMP result1 empty");
            final long firstRead = result1.getLong(1);

            sleep((Long) parameters.get("sleepTime"));

            final ResultSet result2 = runQuery(conn, PostgresQueries.pmp2, parameters);
            if (!result2.next()) throw new IllegalStateException("PMP result2 empty");
            final long secondRead = result2.getLong(1);

            commitTransaction(conn);

            return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void otvInit() {
        createSchema();
        executeUpdates(PostgresQueries.otvInit);
    }

    @Override
    public Map<String, Object> otv1(Map<String, Object> parameters) {
        try (Connection conn = startTransaction()) {
            Random random = new Random();
            for (int i = 0; i < 100; i++) {
                long personId  = random.nextInt((int) parameters.get("cycleSize")+1);
                ResultSet rs = runQuery(conn, PostgresQueries.otv1query, ImmutableMap.of("personId", personId));
                while (rs.next()) {
                    executeUpdates(PostgresQueries.otv1update, ImmutableMap.of("p1id", rs.getLong(1), "p2id", rs.getLong(2), "p3id", rs.getLong(3), "p4id", rs.getLong(4)), true);
                }
                commitTransaction(conn);
            }
            return ImmutableMap.of();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Object> otv2(Map<String, Object> parameters) {
        try (Connection conn = startTransaction()) {
            final ResultSet result1 = runQuery(conn, PostgresQueries.otv2, parameters);
            if (!result1.next()) throw new IllegalStateException("OTV2 result1 empty");
            final List<Object> firstRead = new ArrayList();
            {
                firstRead.add(result1.getLong(1));
                firstRead.add(result1.getLong(2));
                firstRead.add(result1.getLong(3));
                firstRead.add(result1.getLong(4));
            }

            sleep((Long) parameters.get("sleepTime"));

            final ResultSet result2 = runQuery(conn, PostgresQueries.otv2, parameters);
            if (!result2.next()) throw new IllegalStateException("OTV2 result2 empty");
            final List<Object> secondRead = new ArrayList();
            {
                secondRead.add(result2.getLong(1));
                secondRead.add(result2.getLong(2));
                secondRead.add(result2.getLong(3));
                secondRead.add(result2.getLong(4));
            }

            return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void frInit() {

    }

    @Override
    public Map<String, Object> fr1(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public Map<String, Object> fr2(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public void luInit() {

    }

    @Override
    public Map<String, Object> lu1(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public Map<String, Object> lu2(Map<String, Object> parameters) {
        return null;
    }

    @Override
    public void wsInit() {
        createSchema();
        // create 10 pairs of persons with indices (1,2), ..., (19,20)
        for (int i = 1; i <= 10; i++) {
            // we utilise separate transactions for each iteration, but it should not matter in the initialization.
            executeUpdates(PostgresQueries.wsInit, ImmutableMap.of("person1Id", 2*i-1, "person2Id", 2*i), true);
        }
    }

    @Override
    public Map<String, Object> ws1(Map<String, Object> parameters) {
        try (Connection conn = startTransaction()) {
            final ResultSet rs = runQuery(conn, PostgresQueries.ws1query, parameters);

            if (!rs.next()) {
                sleep((Long) parameters.get("sleepTime"));

                long personId = new Random().nextBoolean() ?
                        (long) parameters.get("person1Id") :
                        (long) parameters.get("person2Id");

                executeUpdates(conn, PostgresQueries.ws1update, ImmutableMap.of("personId", personId), true);
            }
            return ImmutableMap.of();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Object> ws2(Map<String, Object> parameters) {
        try (Connection conn = startTransaction()) {
            final ResultSet rs = runQuery(conn, PostgresQueries.ws2, ImmutableMap.of());

            if (rs.next()) {
                return ImmutableMap.of(
                        "p1id",    rs.getLong(1),
                        "p1value", rs.getLong(2),
                        "p2id",    rs.getLong(3),
                        "p2value", rs.getLong(4));
            } else {
                return ImmutableMap.of();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void close() throws Exception {

    }

}
