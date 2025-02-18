package postgres;

public final class PostgresQueries {
    public static final String isolation_serializable = "set transaction isolation level serializable";
    public static final String isolation_repetable_read = "set transaction isolation level repeatable read";
    public static final String isolation_read_committed = "set transaction isolation level read committed";

    public final static String[] tablesCreate = {
            "create table if not exists forum ( id bigint not null, moderatorid bigint )"
            , "create table if not exists post ( id bigint not null, forumid bigint )"
            , "create table if not exists person ( id bigint not null, numFriends bigint, value bigint, version bigint, versionHistory bigint[], name varchar, emails varchar[] )"
            , "create table if not exists likes ( personid bigint not null, postid bigint not null )"
            , "create table if not exists knows ( person1id bigint not null, person2id bigint not null, creationDate varchar, versionHistory bigint[])"
            , "create sequence if not exists id_seq increment by -1 start -1"
    };

    // SQL TRUNCATE does not work az nukeDatabase is also run before CREATEs
    public final static String[] tablesClear = {
            "drop table if exists forum"
            , "drop table if exists post"
            , "drop table if exists person"
            , "drop table if exists likes"
            , "drop table if exists knows"
    };

    public final static String[] atomicityInit = {
            "insert into person (id, name, emails) values " +
                    "(1, 'Alice', ARRAY['alice@aol.com']::varchar[])," +
                    "(2, 'Bob', ARRAY['bob@hotmail.com', 'bobby@yahoo.com']::varchar[])"
    };

    public final static String[] atomicityCTx = {
            "update person set emails = array_append(emails, '$newEmail') where id = $person1Id"
            , "insert into person (id) select '$person2Id' from person where id = $person1Id"
            , "insert into knows (person1id, person2id, creationDate) select p1.id, p2.id, '$creationDate' from person p1, person p2 where p1.id = $person1Id and p2.id = $person2Id"
            , "insert into knows (person1id, person2id, creationDate) select p2.id, p1.id, '$creationDate' from person p1, person p2 where p1.id = $person1Id and p2.id = $person2Id"
    };

    public final static String[] atomicityRBxP1update = {"update person set emails = array_append(emails, '$newEmail') where id = $person1Id"};
    public final static String atomicityRBxP2check = "select id from person where id = $person2Id";
    public final static String[] atomicityRBxP2create = {"insert into person (id, emails) values ($person2Id, ARRAY[]::varchar[])"};

    public final static String atomicityCheck = "select count(*) as numPersons, count(name) as numNames, sum(array_length(emails, 1)) as numEmails from person";

    public final static String[] g0Init = {
            "insert into person (id, versionHistory) values (1, ARRAY[]::bigint[0]), (2, ARRAY[]::bigint[0])"
            , "insert into knows (person1id, person2id, versionHistory) values (1, 2, ARRAY[]::bigint[0]), (2, 1, ARRAY[]::bigint[0])"
    };
    public final static String[] g0 = {
            "update person set versionHistory = versionHistory || $transactionId::bigint where id = $person1Id"
            , "update person set versionHistory = versionHistory || $transactionId::bigint where id = $person2Id"
            , "update knows set versionHistory = versionHistory || $transactionId::bigint where person1id = $person1Id and person2id = $person2Id"
    };

    public final static String g0check =
            "select p1.versionHistory AS p1VersionHistory, k.versionHistory AS kVersionHistory, p2.versionHistory AS p2VersionHistory " +
                    "from person p1, knows k, person p2 " +
                    "where p1.id = k.person1id and p2.id = k.person2id and p1.id = $person1Id and p2.id = $person2Id ";

    public final static String[] g1aInit = { "insert into person (id, version) values (1, 1)" };
    public final static String[] g1a1 = {
            "select id from person where id = $personId"
            , "select pg_sleep($sleepTime / 1000.0)"
            , "update person set version = 2 where id = $personId"
            , "select pg_sleep($sleepTime / 1000.0)"
    };
    public final static String g1a2 = "select version as pVersion from person where id = $personId";

    public final static String[] g1bInit = { "insert into person (id, version) values (1, 99)" };
    public final static String[] g1b1 = {
            "update person set version = $even where id = $personId"
            , "select pg_sleep($sleepTime / 1000.0)"
            , "update person set version = $odd where id = $personId"
    };
    public final static String g1b2 = "select version as pVersion from person where id = $personId";

    public final static String[] g1cInit = { "insert into person (id, version) values (1, 0), (2, 0)"};
    public final static String[] g1c1 = {"update person set version = $transactionId where id = $person1Id"};
    public final static String g1c2 = "select version as person2Version from person where id = $person2Id";

    public final static String[] impInit = { "insert into person (id, version) values (1, 1)" };
    public final static String[] impW = { "update person set version = version + 1 where id = $personId" };
    public final static String impR = "select version as valueRead from person where id = $personId";

    public final static String[] pmpInit = {
            "insert into person (id) values (1)"
            , "insert into post (id) values (1)"
    };
    public final static String[] pmpW = { "insert into likes (personid, postid) select pe.id, po.id from person pe, post po where pe.id = $personId and po.id = $postId" };
    public final static String pmpR = "select count(pe.id) as valueRead from post po, likes l, person pe where po.id = $postId and po.id = l.postid and l.personid = pe.id";

    public final static String[] otvInit = {
            "insert into person (id, version) values (1, 0), (2, 0), (3, 0), (4, 0)"
            , "insert into knows (person1id, person2id) values (1, 2), (2, 3), (3, 4), (4, 1)"
            , "insert into knows (person2id, person1id) values (1, 2), (2, 3), (3, 4), (4, 1)"
    };
    public final static String otvWquery =
            "select p1.id, p2.id, p3.id, p4.id " +
                    "from person p1, person p2, person p3, person p4 " +
                    ", knows k1, knows k2, knows k3, knows k4 " +
                    "where p1.id = $personId " +
                    "and p1.id = k1.person1id and k1.person2id = p2.id " +
                    "and p2.id = k2.person1id and k2.person2id = p3.id " +
                    "and p3.id = k3.person1id and k3.person2id = p4.id " +
                    "and p4.id = k4.person1id and k4.person2id = p1.id " +
                    "and p1.id not in (p2.id, p3.id, p4.id) " +
                    "and p2.id not in (p3.id, p4.id) " +
                    "and p3.id <> p4.id";
    public final static String[] otvWupdate = { "update person set version = version + 1 where id in ($p1id, $p2id, $p3id, $p4id)" };
    public final static String otvR = "select p1.version, p2.version, p3.version, p4.version " +
            "from person p1, person p2, person p3, person p4 " +
            ", knows k1, knows k2, knows k3, knows k4 " +
            "where p1.id = $personId " +
            "and p1.id = k1.person1id and k1.person2id = p2.id " +
            "and p2.id = k2.person1id and k2.person2id = p3.id " +
            "and p3.id = k3.person1id and k3.person2id = p4.id " +
            "and p4.id = k4.person1id and k4.person2id = p1.id " +
            "and p1.id not in (p2.id, p3.id, p4.id) " +
            "and p2.id not in (p3.id, p4.id) " +
            "and p3.id <> p4.id";

    public final static String[] frInit = {
            "insert into person (id, version) values (1, 0), (2, 0), (3, 0), (4, 0)"
            , "insert into knows (person1id, person2id) values (1, 2), (2, 3), (3, 4), (4, 1)"
            // all queries regard knows relationships as directed
            // , "insert into knows (person2id, person1id) values (1, 2), (2, 3), (3, 4), (4, 1)"
    };
    public final static String[] frW = {
            "with " +
                    "path as (select p1.id as p1id, p2.id as p2id, p3.id as p3id, p4.id as p4id " +
                    "from person p1, person p2, person p3, person p4, knows k1, knows k2, knows k3, knows k4 " +
                    "where p1.id = $personId and p1.id = k1.person1id and k1.person2id = p2.id and p2.id = k2.person1id and k2.person2id = p3.id and p3.id = k3.person1id and k3.person2id = p4.id and p4.id = k4.person1id and k4.person2id = p1.id " +
                    "and p1.id not in (p2.id, p3.id, p4.id) and p2.id not in (p3.id, p4.id) and p3.id <> p4.id)" +
                    ", affectedPerson as (select p.id as apid, count(*) as times " +
                    "from person p, path " +
                    "where p.id in (path.p1id, path.p2id, path.p3id, path.p4id) group by p.id) " +
                    "update person " +
                    "set version = version + ap.times " +
                    "from affectedPerson ap " +
                    "where id = ap.apid"
    };
    public final static String frR = "with recursive " +
            "path as (" +
            "select $personId::bigint as endpoint, ARRAY[]::bigint[] nodes, ARRAY[]::bigint[] versions " +
            "union " +
            "select k.person2id, array_append(nodes, k.person2id), array_append(versions, p.version) " +
            "from path, knows k, person p " +
            "where endpoint = k.person1id and k.person2id = p.id and k.person2id <> ALL (nodes) and coalesce(array_length(nodes, 1), 0) < 4" +
            ") " +
            "select endpoint, nodes, versions " +
            "from path " +
            "where endpoint = $personId and array_length(nodes, 1) is not null"
            ;


    public final static String[] luInit = {"insert into person (id, numFriends) values (1, 0)"};
    public final static String[] luW = {
            "with p2 as (insert into person (id, numFriends) values (nextval('id_seq'), 0) returning id) " +
                    ", p1 as (update person set numFriends = numFriends + 1 where id = 1 returning id, numFriends) " +
                    "insert into knows (person1id, person2id) " +
                    "select p1.id, p2.id from p1, p2" // union select p2.id, p1.id from p1, p2"
    };
    public final static String luR = "select count(kp2.person2id) as numKnowsEdges, p1.numFriends as numFriends " +
            "from person p1 left join " +
            "(knows k  inner join person p2 on (k.person2id = p2.id)) kp2 on (p1.id = kp2.person1id) " +
            "where p1.id = $personId " +
            "group by p1.id, p1.numFriends";

    public final static String[] wsInit = { "insert into person (id, value) values ($person1Id, 70), ($person2Id, 80)" };
    public final static String wsWquery = "select p1, id, p2.id from person p1, person p2 where p1.id = $persion1Id and p2.id = $person2Id and p1.value + p2.value >= 100";
    public final static String[] wsWupdate = { "update person set value = value - 100 where id = $personId"};
    public final static String wsR = "select p1.id AS p1id, p1.value AS p1value, p2.id AS p2id, p2.value AS p2value " +
            "from person p1, person p2 " +
            "where p1.id+1 = p2.id " +
            "and p1.value + p2.value <= 0 ";
}
