package postgres;

import java.util.Map;

public final class PostgresQueries {
    public final static String[] tablesCreate = {
            "create table if not exists forum ( id bigint not null, moderatorid bigint )"
            , "create table if not exists post ( id bigint not null, forumid bigint )"
            , "create table if not exists person ( id bigint not null, numfriends bigint, version bigint, versionHistory bigint[], name varchar, emails varchar[] )"
            , "create table if not exists likes ( personid bigint not null, postid bigint not null )"
            , "create table if not exists knows ( person1id bigint not null, person2id bigint not null, versionHistory bigint[])"
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
                    "(1, 'Alice', ARRAY[]::varchar['alice@aol.com']," +
                    "(2, 'Bob', ARRAY[]::varchar['bob@hotmail.com', 'bobby@yahoo.com'])"
    };

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

}
