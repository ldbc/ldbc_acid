package postgres;

public final class PostgresQueries {
    public static final String isolation_serializable = "set transaction isolation level serializable";
    public static final String isolation_repetable_read = "set transaction isolation level repeatable read";
    public static final String isolation_read_committed = "set transaction isolation level read committed";

    public final static String[] tablesCreate = {
            "create table if not exists forum ( id bigint not null, moderatorid bigint )"
            , "create table if not exists post ( id bigint not null, forumid bigint )"
            , "create table if not exists person ( id bigint not null, numfriends bigint, version bigint, versionHistory bigint[], name varchar, emails varchar[] )"
            , "create table if not exists likes ( personid bigint not null, postid bigint not null )"
            , "create table if not exists knows ( person1id bigint not null, person2id bigint not null, creationDate varchar, versionHistory bigint[])"
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
    public final static String[] imp1 = { "update person set version = version + 1 where id = $personId" };
    public final static String imp2 = "select version as valueRead from person where id = $personId";

    public final static String[] pmpInit = {
      "insert into person (id) values (1)"
      , "insert into post (id) values (1)"
    };
    public final static String[] pmp1 = { "insert into likes (personid, postid) select pe.id, po.id from person pe, post po where pe.id = $personId and po.id = $postId" };
    public final static String pmp2 = "select count(pe.id) as valueRead from post po, likes l, person pe where po.id = $postId and po.id = l.postid and l.personid = pe.id";
}
