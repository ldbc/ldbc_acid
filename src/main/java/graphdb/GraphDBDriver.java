package graphdb;

import com.google.common.collect.ImmutableMap;
import driver.TestDriver;
import org.eclipse.rdf4j.http.protocol.Protocol;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryConfigSchema;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.eclipse.rdf4j.repository.manager.RemoteRepositoryManager;
import org.eclipse.rdf4j.repository.manager.RepositoryManager;
import org.eclipse.rdf4j.repository.sail.config.SailRepositorySchema;
import org.eclipse.rdf4j.sail.config.SailConfigSchema;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class GraphDBDriver extends TestDriver<RepositoryConnection, Map<String, Object>, BindingSet> {

	private final RepositoryManager repositoryManager;

	protected HTTPRepository graphDBHTTPRepository;

	private static final String REPOSITORY_ID = "ldbc-snb-acid-test";

	public GraphDBDriver(String endpoint) {
		repositoryManager = new RemoteRepositoryManager(endpoint);
		repositoryManager.init();
		RepositoryConfig config = createRepositoryConfig();
		repositoryManager.addRepositoryConfig(config);
		System.setProperty("http.maxConnections", "200");
		graphDBHTTPRepository = new HTTPRepository(endpoint + File.separatorChar + Protocol.REPOSITORIES
				+ File.separatorChar + REPOSITORY_ID);

	}

	@Override
	public void close() {
		if (graphDBHTTPRepository.getConnection() != null) {
			graphDBHTTPRepository.getConnection().close();
		}
		repositoryManager.removeRepository(REPOSITORY_ID);
		repositoryManager.shutDown();
		System.clearProperty("http.maxConnections");
	}

	@Override
	public RepositoryConnection startTransaction() {
		RepositoryConnection connection = graphDBHTTPRepository.getConnection();
		connection.begin();
		return connection;
	}

	private RepositoryConfig createRepositoryConfig()
			throws RepositoryConfigException {
		Model configGraph = new LinkedHashModel();

		Resource repositoryNode = SimpleValueFactory.getInstance().createBNode("repo");
		Resource repositoryImplNode = SimpleValueFactory.getInstance().createBNode("repoImpl");
		Resource sailImplNode = SimpleValueFactory.getInstance().createBNode("sailImpl");

		configGraph.add(SimpleValueFactory.getInstance()
				.createStatement(repositoryNode, RDF.TYPE, RepositoryConfigSchema.REPOSITORY));
		configGraph.add(SimpleValueFactory.getInstance().createStatement(repositoryNode,
				RepositoryConfigSchema.REPOSITORYID, SimpleValueFactory.getInstance().createLiteral(REPOSITORY_ID)));

		configGraph.add(SimpleValueFactory.getInstance()
				.createStatement(repositoryNode, RepositoryConfigSchema.REPOSITORYIMPL, repositoryImplNode));
		configGraph.add(SimpleValueFactory.getInstance()
				.createStatement(repositoryImplNode, RepositoryConfigSchema.REPOSITORYTYPE,
						SimpleValueFactory.getInstance().createLiteral("openrdf:SailRepository")));
		configGraph.add(SimpleValueFactory.getInstance()
				.createStatement(repositoryImplNode, SailRepositorySchema.SAILIMPL, sailImplNode));
		configGraph.add(SimpleValueFactory.getInstance().createStatement(sailImplNode, SailConfigSchema.SAILTYPE,
				SimpleValueFactory.getInstance().createLiteral("owlim:Sail")));

		return RepositoryConfig.create(configGraph, repositoryNode);
	}

	@Override
	public void commitTransaction(RepositoryConnection tt) {
		tt.commit();
	}

	@Override
	public void abortTransaction(RepositoryConnection tt) {
		tt.rollback();
	}

	@Override
	public BindingSet runQuery(RepositoryConnection tt, String querySpecification, Map<String, Object> stringObjectMap) {
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
		executeUpdate("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
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
				"}");
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
				"    sn:pers%person1Id% snvoc:email \"%newEmail%\" .       \n" +
				"}", parameters));
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
		}
	}

	/**
	 * Dirty Reads: Intermediate Read (Adya’s G1b) anomaly
	 */
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
				commitTransaction(conn);
				return ImmutableMap.of("pVersion", pVersion);
			}
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
			try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"select ?person2Version where { \n" +
							"\tsn:pers%person2Id% snvoc:version ?person2Version .\n" +
							"}", parameters)).evaluate()) {
				if (!queryResult.hasNext()) {
					throw new IllegalStateException("G1C missing person from query result.");
				}
				final BindingSet next = queryResult.next();
				pVersion = Long.parseLong(next.getValue("person2Version").stringValue());
			}
			commitTransaction(conn);
		}
		return ImmutableMap.of("person2Version", pVersion);
	}


	/**
	 * Cut Anomalies: Item-Many-Preceders (IMP) anomaly
	 */
	@Override
	public void impInit() {
		executeUpdate("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
				"PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
				"insert data {\n" +
				"    sn:pers1 rdf:type snvoc:Person ;\n" +
				"             snvoc:id \"1\"^^xsd:long ;\n" +
				"             snvoc:version \"1\"^^xsd:long .\n" +
				"}");
	}

	@Override
	public Map<String, Object> impW(Map<String, Object> parameters) {
		executeUpdate(substituteParameters(
				"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
						"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
						"delete {\n" +
						"    sn:pers%personId% snvoc:version ?v .\n" +
						"} insert {\n" +
						"    sn:pers%personId% snvoc:version ?newV .\n" +
						"} where {\n" +
						"    sn:pers%personId% snvoc:version ?v .\n" +
						"    bind(?v + 1 as ?newV) .\n" +
						"}", parameters));
		return ImmutableMap.of();
	}

	@Override
	public Map<String, Object> impR(Map<String, Object> parameters) {
		long firstRead = 0;
		long secondRead = 0;
		try (RepositoryConnection conn = startTransaction()) {
			try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"select ?firstRead where {\n" +
							"     sn:pers%personId% snvoc:version ?firstRead .\n" +
							"}", parameters)).evaluate()) {
				if (!queryResult.hasNext()) {
					throw new IllegalStateException("IMP missing query result.");
				}
				firstRead = Long.parseLong(queryResult.next().getValue("firstRead").stringValue());
			}
			sleep((Long) parameters.get("sleepTime"));
			try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"select ?secondRead where {\n" +
							"     sn:pers%personId% snvoc:version ?secondRead .\n" +
							"}", parameters)).evaluate()) {
				if (!queryResult.hasNext()) {
					throw new IllegalStateException("IMP missing query result.");
				}
				secondRead = Long.parseLong(queryResult.next().getValue("secondRead").stringValue());
			}
			commitTransaction(conn);
		}
		return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
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
		}
		return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
	}

	/**
	 * Cut Anomalies: Observed Transaction Vanishes (OTV) anomaly
	 */
	@Override
	public void otvInit() {
		initData();
	}

	@Override
	public Map<String, Object> otvW(Map<String, Object> parameters) {
		return initWriteTransactions(parameters);
	}

	@Override
	public Map<String, Object> otvR(Map<String, Object> parameters) {
		return initReadTransactions(parameters);
	}

	/**
	 * Cut Anomalies: Fractured Read (FR) anomaly
	 */
	@Override
	public void frInit() {
		initData();
	}

	@Override
	public Map<String, Object> frW(Map<String, Object> parameters) {
		return initWriteTransactions(parameters);
	}

	@Override
	public Map<String, Object> frR(Map<String, Object> parameters) {
		return initReadTransactions(parameters);
	}

	private void initData() {
		try (RepositoryConnection conn = startTransaction()) {
			for (int i = 1; i <= 4; i++) {
				conn.prepareUpdate("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
						"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
						"insert data {\n" +
						"    sn:pers" + i + " snvoc:version \"0\"^^xsd:integer .\n" +
						"    sn:pers" + i + " snvoc:id \"" + i + "\"^^xsd:long .\n" +
						"    sn:pers" + i + " snvoc:knows sn:pers" + (i == 4 ? 1 : i + 1) + " .\n" +
						"    sn:pers" + (i == 4 ? 1 : i + 1) + " snvoc:knows sn:pers" + i + " .\n" +
						"}").execute();
			}
			commitTransaction(conn);
		}
	}

	private Map<String, Object> initWriteTransactions(Map<String, Object> parameters) {
		Random random = new Random();
		for (int i = 0; i < 100; i++) {
			int max = (int) parameters.get("cycleSize") + 1;
			long personId = random.nextInt(max - 1) + 1;
			try (RepositoryConnection conn = startTransaction()) {
				long pVersionIncreased;
				try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL,
						"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
								"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
								"select ?v where {\n" +
								"    ?p snvoc:id \"" + personId + "\"^^xsd:long .\n" +
								"    ?p snvoc:version ?v .\n" +
								"}").evaluate()) {
					if (!queryResult.hasNext()) {
						throw new IllegalStateException("Missing person from query result.");
					}
					pVersionIncreased = Long.parseLong(queryResult.next().getValue("v").stringValue()) + 1;
				}
				conn.prepareUpdate(QueryLanguage.SPARQL,
						"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
								"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
								"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
								"delete {\n" +
								"    ?p1 snvoc:version ?v1 .\n" +
								"    ?p2 snvoc:version ?v2 .\n" +
								"    ?p3 snvoc:version ?v3 .\n" +
								"    ?p4 snvoc:version ?v4 .\n" +
								"}\n" +
								"insert {\n" +
								"    ?p1 snvoc:version \"" + pVersionIncreased + "\"^^xsd:long .\n" +
								"    ?p2 snvoc:version \"" + pVersionIncreased + "\"^^xsd:long .\n" +
								"    ?p3 snvoc:version \"" + pVersionIncreased + "\"^^xsd:long .\n" +
								"    ?p4 snvoc:version \"" + pVersionIncreased + "\"^^xsd:long .\n" +
								"} where {\n" +
								"    ?p1 snvoc:id \"" + personId + "\"^^xsd:long .\n" +
								"    ?p1 snvoc:knows ?p2 ;\n" +
								"        snvoc:version ?v1 .\n" +
								"    ?p2 snvoc:knows ?p3 ;\n" +
								"        snvoc:version ?v2 .\n" +
								"    ?p3 snvoc:knows ?p4 ;\n" +
								"        snvoc:version ?v3 .\n" +
								"    ?p4 snvoc:knows ?p1 ;\n" +
								"        snvoc:version ?v4 .\n" +
								"}").execute();
				commitTransaction(conn);
			}
		}
		return ImmutableMap.of();
	}

	private Map<String, Object> initReadTransactions(Map<String, Object> parameters) {
		final List<Object> firstRead = new ArrayList<>();
		final List<Object> secondRead = new ArrayList<>();
		try (RepositoryConnection conn = startTransaction()) {
			try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
							"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"select ?v1 ?v2 ?v3 ?v4 where {\n" +
							"    ?p1 snvoc:id \"%personId%\"^^xsd:long .\n" +
							"    ?p1 snvoc:knows ?p2 ; snvoc:version ?v1 .\n" +
							"    ?p2 snvoc:knows ?p3 ; snvoc:version ?v2 .\n" +
							"    ?p3 snvoc:knows ?p4 ; snvoc:version ?v3 .\n" +
							"    ?p4 snvoc:knows ?p1 ; snvoc:version ?v4 .\n" +
							"}", parameters)).evaluate()) {
				if (!queryResult.hasNext()) {
					throw new IllegalStateException("Missing query result.");
				}

				firstRead.add(Long.parseLong(queryResult.next().getValue("v1").stringValue()));
				firstRead.add(Long.parseLong(queryResult.next().getValue("v2").stringValue()));
				firstRead.add(Long.parseLong(queryResult.next().getValue("v3").stringValue()));
				firstRead.add(Long.parseLong(queryResult.next().getValue("v4").stringValue()));
			}
			sleep((Long) parameters.get("sleepTime"));
			try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
							"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"select ?v1 ?v2 ?v3 ?v4 where {\n" +
							"    ?p1 snvoc:id \"%personId%\"^^xsd:long .\n" +
							"    ?p1 snvoc:knows ?p2 ; snvoc:version ?v1 .\n" +
							"    ?p2 snvoc:knows ?p3 ; snvoc:version ?v2 .\n" +
							"    ?p3 snvoc:knows ?p4 ; snvoc:version ?v3 .\n" +
							"    ?p4 snvoc:knows ?p1 ; snvoc:version ?v4 .\n" +
							"}", parameters)).evaluate()) {
				if (!queryResult.hasNext()) {
					throw new IllegalStateException("Missing query result.");
				}
				secondRead.add(Long.parseLong(queryResult.next().getValue("v1").stringValue()));
				secondRead.add(Long.parseLong(queryResult.next().getValue("v2").stringValue()));
				secondRead.add(Long.parseLong(queryResult.next().getValue("v3").stringValue()));
				secondRead.add(Long.parseLong(queryResult.next().getValue("v4").stringValue()));
			}
			commitTransaction(conn);
		}
		return ImmutableMap.of("firstRead", firstRead, "secondRead", secondRead);
	}

	/**
	 * Cut Anomalies: Lost Update (LU) anomaly
	 */
	@Override
	public void luInit() {
		executeUpdate("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
				"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
				"insert data {\n" +
				"    sn:pers1 snvoc:id \"1\"^^xsd:long ;\n" +
				"             rdf:type snvoc:Person ;\n" +
				"             snvoc:numFriends \"0\"^^xsd:long .\n" +
				"}");
	}

	@Override
	public Map<String, Object> luW(Map<String, Object> parameters) {
		try (RepositoryConnection conn = startTransaction()) {
			conn.prepareUpdate(substituteParameters("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
					"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
					"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
					"delete {\n" +
					"    sn:pers%person1Id% snvoc:numFriends ?n .\n" +
					"} insert {\n" +
					"    sn:pers%person1Id% snvoc:knows sn:pers%person2Id% .\n" +
					"    sn:pers%person1Id% snvoc:numFriends ?incrFr .\n" +
					"} where {\n" +
					"    sn:pers%person1Id% snvoc:numFriends ?n .\n" +
					"    bind(?n + 1 as ?incrFr) .\n" +
					"}", parameters)).execute();
			commitTransaction(conn);
		}
		return ImmutableMap.of();
	}

	@Override
	public Map<String, Object> luR(Map<String, Object> parameters) {
		long numKnowsEdges = 0;
		long numFriends = 0;
		try (RepositoryConnection conn = startTransaction()) {
			try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
							"select (count (snvoc:knows) as ?numKnowsEdges) ?numFriendsProp where {\n" +
							"    ?p snvoc:id \"%personId%\"^^xsd:long .\n" +
							"    ?p snvoc:knows ?fr .\n" +
							"    ?p snvoc:numFriends ?numFriendsProp . \n" +
							"} group by ?p ?numFriendsProp", parameters)).evaluate()) {
				if (!queryResult.hasNext()) {
					throw new IllegalStateException("OTV missing query result.");
				}
				final BindingSet next = queryResult.next();
				numKnowsEdges = Long.parseLong(next.getValue("numKnowsEdges").stringValue());
				numFriends = Long.parseLong(next.getValue("numFriendsProp").stringValue());
			}
			commitTransaction(conn);
		}
		return ImmutableMap.of("numKnowsEdges", numKnowsEdges, "numFriendsProp", numFriends);
	}

	/**
	 * Cut Anomalies: Write Skew (WS) anomaly
	 */
	@Override
	public void wsInit() {
		try (RepositoryConnection conn = startTransaction()) {
			for (int i = 1; i <= 10; i++) {
				conn.prepareUpdate(substituteParameters("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
						"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
						"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
						"insert data {\n" +
						"    sn:pers%person1Id% snvoc:id \"%person1Id%\"^^xsd:long ;\n" +
						"             snvoc:value \"70\"^^xsd:integer .\n" +
						"    sn:pers%person2Id% snvoc:id \"%person2Id%\"^^xsd:long ;\n" +
						"             snvoc:value \"80\"^^xsd:integer .\n" +
						"}", ImmutableMap.of("person1Id", 2 * i - 1, "person2Id", 2 * i))).execute();
			}
			commitTransaction(conn);
		}
	}

	@Override
	public Map<String, Object> wsW(Map<String, Object> parameters) {
		try (RepositoryConnection conn = startTransaction()) {
			try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"select ?p1Id ?p2Id where {\n" +
							"    sn:pers%person1Id% snvoc:version ?p1v ;\n" +
							"             snvoc:id ?p1Id .\n" +
							"    sn:pers%person2Id% snvoc:version ?p2v ;\n" +
							"             snvoc:id ?p2Id .\n" +
							"    filter (?p1v + ?p2v >= 100)\n" +
							"}", parameters)).evaluate()) {
				if (queryResult.hasNext()) {
					sleep((Long) parameters.get("sleepTime"));

					long personId = new Random().nextBoolean() ?
							(long) parameters.get("person1Id") :
							(long) parameters.get("person2Id");

					conn.prepareUpdate(substituteParameters("PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"delete {\n" +
							"  sn:pers%personId% snvoc:value ?v .  \n" +
							"} insert {\n" +
							"    sn:pers%personId% snvoc:value ?newV .  \n" +
							"} where {\n" +
							"    sn:pers%personId% snvoc:value ?v .  \n" +
							"    bind(?v - 100 as ?newV)\n" +
							"}", ImmutableMap.of("personId", personId))).execute();
					commitTransaction(conn);
				}
			}
			commitTransaction(conn);
		}
		return ImmutableMap.of();
	}

	@Override
	public Map<String, Object> wsR(Map<String, Object> parameters) {
		try (RepositoryConnection conn = startTransaction()) {
			try (TupleQueryResult queryResult = conn.prepareTupleQuery(QueryLanguage.SPARQL, substituteParameters(
					"PREFIX sn: <http://www.ldbc.eu/ldbc_socialnet/1.0/data/>\n" +
							"PREFIX snvoc: <http://www.ldbc.eu/ldbc_socialnet/1.0/vocabulary/>\n" +
							"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
							"select ?p1id ?p1value ?p2value ?p2id where {\n" +
							"    ?p1 snvoc:id ?p1id ;\n" +
							"             snvoc:value ?p1value .       \n" +
							"    bind(?p1id + 1 as ?p2id) .\n" +
							"    ?p2 snvoc:id ?p2id ;\n" +
							"             snvoc:value ?p2value .              \n" +
							"    filter (?p1value + ?p2value <= 0) .\n" +
							"}", parameters)).evaluate()) {
				if (queryResult.hasNext()) {
					final BindingSet next = queryResult.next();
					return ImmutableMap.of(
							"p1id", next.getValue("p1id"),
							"p1value", next.getValue("p1value"),
							"p2id", next.getValue("p2id"),
							"p2value", next.getValue("p2value"));
				}
			}
			commitTransaction(conn);
		}
		return ImmutableMap.of();
	}

	public void executeUpdate(String querySpecification) {
		try (RepositoryConnection conn = startTransaction()) {
			conn.prepareUpdate(QueryLanguage.SPARQL, querySpecification).execute();
			commitTransaction(conn);
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
}
