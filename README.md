# LDBC ACID tests

## Systems

* Neo4j
* Memgraph
* Dgraph
* JanusGraph (various backends)
* PostgreSQL (various isolation levels)

## Paper

* Source: <https://github.com/bme-db-lab/paper-snb-interactive-tpctc>
* PDF: <https://www.db.bme.hu/paper-snb-interactive-tpctc/ms.pdf>

## Getting started

### Neo4j

Version 3.5.x:
```bash
docker run -p 7474:7474 -p 7687:7687 --env=NEO4J_AUTH=none neo4j:3.5.20
```

Version 4.1.x:
```bash
docker run -p 7474:7474 -p 7687:7687 --env=NEO4J_AUTH=none neo4j:4.1.1
```

### Memgraph

Bind it to `17687` to avoid a collision with Neo4j.

```bash
wget https://download.memgraph.com/memgraph/v1.1.0/docker/memgraph-1.1.0-community-docker.tar.gz
docker load -i memgraph-1.1.0-community-docker.tar.gz
docker run -p 17687:7687 memgraph
```

If you need to change the configuration, use
```bash
docker run -p 17687:7687 -v mg_lib:/var/lib/memgraph -v mg_log:/var/log/memgraph -v mg_etc:/etc/memgraph memgraph
```

Experimenting: the Neo4j Cypher shell works with Memgraph.

```console
$ bin/cypher-shell 
Connected to Neo4j 3.0.0 at bolt://localhost:7687.
Type :help for a list of available commands or :exit to exit the shell.
Note that Cypher queries must end with a semicolon.
```

```
sudo snap install node --classic --channel=12
```

### Dgraph

```bash
docker run --rm -it -p 8080:8080 -p 9080:9080 -p 8000:8000 -v ~/dgraph:/dgraph dgraph/standalone:v20.07.0
```

### Postgres

We're currently using an embedded version.
