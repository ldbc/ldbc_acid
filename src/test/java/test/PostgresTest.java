package test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import postgres.PostgresDriver;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;

import java.io.IOException;

import static ru.yandex.qatools.embed.postgresql.distribution.Version.Main.*;

public class PostgresTest extends AcidTest<PostgresDriver> {

    static EmbeddedPostgres postgres;

    public PostgresTest() {
        super(new PostgresDriver());
    }

    @BeforeClass
    public static void setUp() throws IOException {
        postgres = new EmbeddedPostgres(V9_6);
        final String url = postgres.start("localhost", 5432, "dbName", "userName", "password");
        // TODO
    }

    @AfterClass
    public static void tearDown() {
        postgres.stop();
    }

}
