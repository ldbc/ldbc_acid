# LDBC ACID tests

[TPCTC'20 paper](https://link.springer.com/chapter/10.1007/978-3-030-84924-5_1) ([preprint PDF](http://mit.bme.hu/~szarnyas/ldbc/ldbc-acid-tpctc2020-camera-ready.pdf))

See the chapter on "ACID tests" in the [LDBC SNB specification](https://ldbc.github.io/ldbc_snb_docs/ldbc-snb-specification.pdf)

## Systems

* Neo4j
* Memgraph
* Dgraph
* JanusGraph (various backends)
* PostgreSQL (various isolation levels)

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

### TigerGraph

```bash
#docker stop acid-tg # Stop the docker after the test
docker run --rm -idt --name acid-tg -p 9000:9000 -p 14022:22 -p 14240:14240 -v `pwd`/tigergraph:/tigergraph:z docker.tigergraph.com/tigergraph:latest
docker exec --user tigergraph -it acid-tg /home/tigergraph/tigergraph/app/cmd/gadmin start all

# go to the container
docker exec --user tigergraph -it acid-tg bash
```