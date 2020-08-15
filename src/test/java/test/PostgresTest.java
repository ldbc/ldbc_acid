package test;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import postgres.PostgresDriver;
import ru.yandex.qatools.embed.postgresql.EmbeddedPostgres;
import ru.yandex.qatools.embed.postgresql.config.PostgresConfig;

import java.io.IOException;

import static ru.yandex.qatools.embed.postgresql.distribution.Version.Main.V9_6;

public class PostgresTest extends AcidTest<PostgresDriver> {

    static EmbeddedPostgres postgres;

    public PostgresTest() {
        super(new PostgresDriver());
    }

    @Override
    public void initialize() {
        PostgresConfig c = postgres.getConfig().get();
        testDriver.initDataSource(c.net().host(), c.net().port(), c.credentials().username(), c.credentials().password(), c.storage().dbName());

        super.initialize();
    }

    @BeforeClass
    public static void setUp() throws IOException {
        postgres = new EmbeddedPostgres(V9_6);
        final String url = postgres.start();
        // TODO
    }

    @AfterClass
    public static void tearDown() {
        postgres.stop();
    }

}
