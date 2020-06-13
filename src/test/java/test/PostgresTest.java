package test;

import org.junit.BeforeClass;
import postgres.PostgresDriver;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;

import java.io.IOException;

import static ru.yandex.qatools.embed.postgresql.distribution.Version.Main.*;

public class PostgresTest extends AcidTest<PostgresDriver> {

    public PostgresTest() {
        super(new PostgresDriver());
    }

    @BeforeClass
    public static void setUp() throws IOException {
        final EmbeddedPostgres postgres = new EmbeddedPostgres(V9_6);
        final String url = postgres.start("localhost", 5432, "dbName", "userName", "password");
        // TODO
    }

}
