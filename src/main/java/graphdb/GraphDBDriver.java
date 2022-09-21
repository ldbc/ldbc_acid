package graphdb;

import com.google.api.Http;
import com.google.common.collect.ImmutableMap;
import driver.TestDriver;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class GraphDBDriver extends TestDriver<RepositoryConnection, Map<String, Object>, BindingSet> {

	protected final HTTPRepository graphDBHTTPRepository;

	public GraphDBDriver(String endpoint) {
		graphDBHTTPRepository = new HTTPRepository(endpoint);
	}

	@Override
	public void close() {
		if (graphDBHTTPRepository.getConnection() != null) {
			graphDBHTTPRepository.getConnection().close();
		}
	}

	@Override
	public RepositoryConnection startTransaction() {
		//HTTPRepository http = new HTTPRepository("http://localhost:7201/repositories/ldbc-snb-acid-test");
		final RepositoryConnection connection = graphDBHTTPRepository.getConnection();
		connection.begin();
		return connection;
	}

	@Override
	public void commitTransaction(RepositoryConnection tt) {
		tt.commit();
	}

	@Override
	public void abortTransaction(RepositoryConnection tt) {
		tt.rollback(); //todo: abort
	}

	@Override
	public BindingSet runQuery(RepositoryConnection tt, String querySpecification, Map<String, Object> stringObjectMap) {
		//return tt.prepareTupleQuery(QueryLanguage.SPARQL, querySpecification).evaluate();
		return null;
	}

	@Override
	public void nukeDatabase() {
		try (RepositoryConnection repositoryConnection = startTransaction()) {
			repositoryConnection.prepareUpdate("CLEAR ALL").execute();
			commitTransaction(repositoryConnection);
		}
	}

	/**
	 * Atomicity anomalies
	 */

	@Override
	public void atomicityInit() {
		try (RepositoryConnection repositoryConnection = startTransaction()) {
			repositoryConnection.prepareUpdate("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/> \n" +
					"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>" +
					"INSERT DATA {\n" +
					"    sn:pers1\n" +
					"        rdf:type snvoc:Person ;\n" +
					"        snvoc:id \"1\"^^xsd:long ;\n" +
					"        snvoc:name \"Alice\" ;\n" +
					"        snvoc:email \"alice@aol.com\" .\n" +
					"    sn:pers2\n" +
					"        rdf:type snvoc:Person ;\n" +
					"        snvoc:id \"2\"^^xsd:long ;\n" +
					"        snvoc:name \"Bob\" ;\n" +
					"        snvoc:email \"bob@hotmail.com\" ;\n" +
					"        snvoc:email \"bobby@yahoo.com\" .\n" +
					"}").execute();
			commitTransaction(repositoryConnection);
		}
	}

	@Override
	public void atomicityC(Map<String, Object> parameters) {
		executeUpdate(substituteParameters("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
				"INSERT DATA {\n" +
				"    sn:pers%person1Id% snvoc:knows _:knows .\n" +
				"    _:knows snvoc:hasPerson sn:pers%person2Id% .\n" +
				"    sn:pers%person2Id% rdf:type snvoc:Person .\n" +
				"    _:knows snvoc:creationDate \"%since%\"^^xsd:integer .   \n" +
				"    sn:pers%person2Id% snvoc:id \"%person2Id%\"^^xsd:long .\n" +
				"    sn:pers%person1Id% snvoc:email \"%$newEmail%\" .       \n" +
				"}", parameters));
	}

	public void executeUpdate(String querySpecification) {
		try (RepositoryConnection conn = startTransaction()) {
			conn.prepareUpdate(QueryLanguage.SPARQL, querySpecification).execute();
			commitTransaction(conn);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String substituteParameters(String querySpecification, Map<String, Object> stringStringMap) {
		if (stringStringMap != null) {
			for (Map.Entry<String, Object> param : stringStringMap.entrySet()) {
				querySpecification = querySpecification.replace("%" + param.getKey() + "%", param.getValue().toString());
			}
		}
		return querySpecification;
	}

	@Override
	public void atomicityRB(Map<String, Object> parameters) {
		try (RepositoryConnection repositoryConnection = startTransaction()) {
			repositoryConnection.prepareUpdate(substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"INSERT DATA {\n" +
							"    sn:pers%person1Id% snvoc:email \"%newEmail%\" . \n" +
							"} ", parameters)).execute();

			try (TupleQueryResult queryResult = repositoryConnection.prepareTupleQuery(substituteParameters(
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
							"select ?p where { \n" +
							"\t?p snvoc:id \"%person2Id%\"^^xsd:long .    \n" +
							"}", parameters)).evaluate()) {
				if (queryResult.hasNext()) {
					abortTransaction(repositoryConnection);
				} else {
					repositoryConnection.prepareUpdate(substituteParameters(
							"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
									"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
									"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
									"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
									"INSERT DATA {\n" +
									"    sn:pers%person2Id% rdf:type snvoc:Person ;\n" +
									"             snvoc:id \"%person2Id%\"^^xsd:long .\n" +
									"}", parameters)).execute();
					commitTransaction(repositoryConnection);
				}
			}
		}
	}

	@Override
	public Map<String, Object> atomicityCheck() {
		try (RepositoryConnection conn = startTransaction()) {
			try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL,
					"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"select (count (distinct ?p) as ?numPersons) (count (distinct ?name) as ?numNames) (count (distinct ?emails) as ?numEmails) where {\n" +
							"    ?p rdf:type snvoc:Person .\n" +
							"    optional {\n" +
							"        ?p snvoc:name ?name .\n" +
							"    }\n" +
							"    optional {\n" +
							"        ?p snvoc:email ?emails .\n" +
							"    }\n" +
							"}").evaluate()) {
				BindingSet bindingSet = queryResult.next();
				long numPersons = Long.parseLong(bindingSet.getValue("numPersons").stringValue());
				long numNames = Long.parseLong(bindingSet.getValue("numNames").stringValue());
				long numEmails = Long.parseLong(bindingSet.getValue("numEmails").stringValue());

				commitTransaction(conn);
				return ImmutableMap.of("numPersons", numPersons, "numNames", numNames, "numEmails", numEmails);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Dirty Write (Adya’s G0) anomaly
	 */

	@Override
	public void g0Init() {
		executeUpdate("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
				"insert data { \n" +
				"\tsn:pers1 snvoc:knows _:knows .\n" +
				"    _:knows snvoc:hasPerson sn:pers2 .\n" +
				"    sn:pers1 snvoc:versionHistory 0 .\n" +
				"    sn:pers2 snvoc:versionHistory 0 .\n" +
				"    _:knows snvoc:versionHistory 0 . \n" +
				"}\n");
	}

	@Override
	public Map<String, Object> g0(Map<String, Object> parameters) {
		executeUpdate(substituteParameters("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
				"insert { \n" +
				"    sn:pers%person1Id% snvoc:versionHistory \"%transactionId%\"^^xsd:long .\n" +
				"    sn:pers%person2Id% snvoc:versionHistory \"%transactionId%\"^^xsd:long .\n" +
				"    ?knows snvoc:versionHistory \"%transactionId%\"^^xsd:long .  \n" +
				"} where {\n" +
				"    sn:pers%person1Id% snvoc:knows ?knows .\n" +
				"    ?knows snvoc:hasPerson sn:pers%person2Id% .\n" +
				"}", parameters));
		return ImmutableMap.of();
	}

	@Override
	public Map<String, Object> g0check(Map<String, Object> parameters) {
		final List<Object> p1VersionHistory = new ArrayList<>();
		final List<Object> kVersionHistory = new ArrayList<>();
		final List<Object> p2VersionHistory = new ArrayList<>();
		try (RepositoryConnection conn = startTransaction()) {
			try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"select ?p1VersionHistory where {\n" +
							"    sn:pers%person1Id% snvoc:versionHistory ?p1VersionHistory .\n" +
							"}", parameters)).evaluate()) {
				while (queryResult.hasNext()) {
					p1VersionHistory.add(queryResult.next().getValue("p1VersionHistory").stringValue());
				}
			}
			try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"select ?p2VersionHistory where {\n" +
							"    sn:pers%person2Id% snvoc:versionHistory ?p2VersionHistory .\n" +
							"}", parameters)).evaluate()) {
				while (queryResult.hasNext()) {
					p2VersionHistory.add(queryResult.next().getValue("p2VersionHistory").stringValue());
				}
			}
			try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"select ?kVersionHistory where {\n" +
							"    sn:pers%person1Id% snvoc:knows _:knows .\n" +
							"    _:knows snvoc:hasPerson sn:pers%person2Id% .\n" +
							"    _:knows snvoc:versionHistory ?kVersionHistory .\n" +
							"}", parameters)).evaluate()) {
				while (queryResult.hasNext()) {
					kVersionHistory.add(queryResult.next().getValue("kVersionHistory").stringValue());
				}
			}
			commitTransaction(conn);
			return ImmutableMap.of("p1VersionHistory", p1VersionHistory, "p2VersionHistory", p2VersionHistory, "kVersionHistory", kVersionHistory);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Dirty Reads: Aborted Read (G1a) anomaly
	 */

	@Override
	public void g1aInit() {
		executeUpdate("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
				"insert data {\n" +
				"    sn:pers1 rdf:type snvoc:Person ;\n" +
				"             snvoc:id \"1\"^^xsd:long ;\n" +
				"             snvoc:version \"1\"^^xsd:integer .\n" +
				"}");
	}

	@Override
	public Map<String, Object> g1aW(Map<String, Object> parameters) {
		try (RepositoryConnection conn = startTransaction()) {
			long personId;
			try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"select ?id where { \n" +
							"\tsn:pers%personId% snvoc:id ?id .\n" +
							"}", parameters)).evaluate()) {
				if (!queryResult.hasNext()) {
					throw new IllegalStateException("G1a TW missing person from query result.");
				}
				personId = Long.parseLong(queryResult.next().getValue("id").stringValue());
			}
			sleep((Long) parameters.get("sleepTime"));
			conn.prepareUpdate(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
							"delete {\n" +
							"    sn:pers%personId% snvoc:version ?v .\n" +
							"} insert {\n" +
							"    sn:pers%personId% snvoc:version \"2\"^^xsd:integer .\n" +
							"} where {\n" +
							"    sn:pers%personId% snvoc:version ?v .\n" +
							"}", ImmutableMap.of("personId", personId))).execute();
			sleep((Long) parameters.get("sleepTime"));
			abortTransaction(conn);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return ImmutableMap.of();
	}

	@Override
	public Map<String, Object> g1aR(Map<String, Object> parameters) {
		try (RepositoryConnection conn = startTransaction()) {
			try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"select ?pVersion where { \n" +
							"\tsn:pers%personId% snvoc:version ?pVersion .\n" +
							"}", parameters)).evaluate()) {
				if (!queryResult.hasNext()) {
					throw new IllegalStateException("G1A2 missing person from query result.");
				}
				final long pVersion = Long.parseLong(queryResult.next().getValue("pVersion").stringValue());
				commitTransaction(conn);
				return ImmutableMap.of("pVersion", pVersion);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Dirty Reads: Intermediate Read (Adya’s G1b) anomaly
	 */

	//todo: fix connections
	@Override
	public void g1bInit() {
		executeUpdate("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
				"insert data {\n" +
				"    sn:pers1 rdf:type snvoc:Person ;\n" +
				"             snvoc:id \"1\"^^xsd:long ;\n" +
				"             snvoc:version \"99\"^^xsd:integer .\n" +
				"}");
	}

	@Override
	public Map<String, Object> g1bW(Map<String, Object> parameters) {
		try (RepositoryConnection conn = startTransaction()) {
			conn.prepareUpdate(QueryLanguage.SPARQL, substituteParameters("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
					"delete {\n" +
					"    sn:pers%personId% snvoc:version ?p .\n" +
					"} insert {\n" +
					"    sn:pers%personId% snvoc:version \"%even%\"^^xsd:integer .\n" +
					"} where {\n" +
					"    sn:pers%personId% snvoc:version ?p .\n" +
					"}", parameters)).execute();
			sleep((Long) parameters.get("sleepTime"));
			conn.prepareUpdate(QueryLanguage.SPARQL, substituteParameters("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
					"delete {\n" +
					"    sn:pers%personId% snvoc:version ?p .\n" +
					"} insert {\n" +
					"    sn:pers%personId% snvoc:version \"%odd%\"^^xsd:integer .\n" +
					"} where {\n" +
					"    sn:pers%personId% snvoc:version ?p .\n" +
					"}", parameters)).execute();
			commitTransaction(conn);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return ImmutableMap.of();
	}

	@Override
	public Map<String, Object> g1bR(Map<String, Object> parameters) {
		try (RepositoryConnection conn = startTransaction()) {
			try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"select ?pVersion where { \n" +
							"\tsn:pers%personId% snvoc:version ?pVersion .\n" +
							"}", parameters)).evaluate()) {
				if (!queryResult.hasNext()) {
					throw new IllegalStateException("G1B missing person from query result.");
				}
				final long pVersion = Long.parseLong(queryResult.next().getValue("pVersion").stringValue());
				return ImmutableMap.of("pVersion", pVersion);
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Dirty Reads: Circular Information Flow (Adya’s G1c) anomaly
	 */

	@Override
	public void g1cInit() {
		executeUpdate("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/> \n" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
				"INSERT DATA {\n" +
				"    sn:pers1\n" +
				"        rdf:type snvoc:Person ;\n" +
				"        snvoc:id \"1\"^^xsd:long ;\n" +
				"\t\tsnvoc:version \"0\"^^xsd:integer . \n" +
				"    sn:pers2\n" +
				"        rdf:type snvoc:Person ;\n" +
				"        snvoc:id \"2\"^^xsd:long ;\n" +
				"\t\tsnvoc:version \"0\"^^xsd:integer .\n" +
				"}");
	}

	@Override
	public Map<String, Object> g1c(Map<String, Object> parameters) {
		long pVersion;
		try (RepositoryConnection conn = startTransaction()) {
			conn.prepareUpdate(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
							"delete {\n" +
							"    sn:pers%person1Id% snvoc:version ?p .\n" +
							"} insert {\n" +
							"    sn:pers%person1Id% snvoc:version \"%transactionId%\"^^xsd:integer .\n" +
							"} where {\n" +
							"    sn:pers%person1Id% snvoc:version ?p .\n" +
							"}", parameters)).execute();
			TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"select ?person2Version where { \n" +
							"\tsn:pers%person2Id% snvoc:version ?person2Version .\n" +
							"}", parameters)).evaluate();
			if (!queryResult.hasNext()) {
				throw new IllegalStateException("G1C missing person from query result.");
			}
			final BindingSet next = queryResult.next();
			pVersion = Long.parseLong(next.getValue("person2Version").stringValue());
			commitTransaction(conn);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		return ImmutableMap.of("person2Version", pVersion);
	}

	@Override
	public void impInit() {
	}

	@Override
	public Map<String, Object> impW(Map<String, Object> parameters) {
		return null;
	}

	@Override
	public Map<String, Object> impR(Map<String, Object> parameters) {
		return null;
	}

	/**
	 * Cut Anomalies: Item-Many-Preceders (IMP) anomaly
	 */

	@Override
	public void pmpInit() {
		executeUpdate("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/> \n" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
				"INSERT DATA {\n" +
				"    sn:pers1\n" +
				"        rdf:type snvoc:Person ;\n" +
				"        snvoc:id \"1\"^^xsd:long .\n" +
				"    sn:post1\n" +
				"        rdf:type snvoc:Post ;\n" +
				"        snvoc:id \"1\"^^xsd:long .\n" +
				"}");
	}

	@Override
	public Map<String, Object> pmpW(Map<String, Object> parameters) {
		executeUpdate(substituteParameters("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
				"insert data {\n" +
				"     sn:pers%personId% snvoc:likes sn:post%postId% .\n" +
				"} ", parameters));
		return ImmutableMap.of();
	}

	@Override
	public Map<String, Object> pmpR(Map<String, Object> parameters) {
		long firstRead;
		long secondRead;
		try (RepositoryConnection conn = startTransaction()) {
			conn.begin();
			try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"select (count(sn:pers%personId%) as ?firstRead) where {\n" +
							"    sn:pers%personId% snvoc:likes sn:post%postId% .\n" +
							"}", parameters)).evaluate()) {
				if (!queryResult.hasNext()) {
					throw new IllegalStateException("PMP missing query result.");
				}
				firstRead = Long.parseLong(queryResult.next().getValue("firstRead").stringValue());
			}
			sleep((Long) parameters.get("sleepTime"));
			try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"select (count(sn:pers%personId%) as ?secondRead) where {\n" +
							"    sn:pers%personId% snvoc:likes sn:post%postId% .\n" +
							"}", parameters)).evaluate()) {
				if (!queryResult.hasNext()) {
					throw new IllegalStateException("PMP missing query result.");
				}
				secondRead = Long.parseLong(queryResult.next().getValue("secondRead").stringValue());
			}
			commitTransaction(conn);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
	}

	/**
	 * Cut Anomalies: Observed Transaction Vanishes (OTV) anomaly
	 */
	@Override
	public void otvInit() {
		try (RepositoryConnection conn = startTransaction()) {
			for (int i = 1; i <= 4; i++) {
				conn.prepareUpdate("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
						"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
						"insert data {\n" +
						"    sn:pers" + i + " snvoc:version \"0\"^^xsd:integer .\n" +
						"    sn:pers" + i + " snvoc:knows sn:pers" + (i == 4 ? 1 : i + 1) + " .\n" +
						"    sn:pers" + (i == 4 ? 1 : i + 1) + " snvoc:knows  sn:pers" + i + " .\n" +
						"}").execute();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public Map<String, Object> otvW(Map<String, Object> parameters) {
		return null;
	}

	@Override
	public Map<String, Object> otvR(Map<String, Object> parameters) {
		return null;
	}

	@Override
	public void frInit() {

	}

	@Override
	public Map<String, Object> frW(Map<String, Object> parameters) {
		return null;
	}

	@Override
	public Map<String, Object> frR(Map<String, Object> parameters) {
		return null;
	}

	@Override
	public void luInit() {

	}

	@Override
	public Map<String, Object> luW(Map<String, Object> parameters) {
		return null;
	}

	@Override
	public Map<String, Object> luR(Map<String, Object> parameters) {
		return null;
	}

	@Override
	public void wsInit() {

	}

	@Override
	public Map<String, Object> wsW(Map<String, Object> parameters) {
		return null;
	}

	@Override
	public Map<String, Object> wsR(Map<String, Object> parameters) {
		return null;
	}
}
