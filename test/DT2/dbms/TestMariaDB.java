package DT2.dbms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import DT2.Main;

public class TestMariaDB {

    @Test
    public void testMariaDB() {
        String mariaDBAvailable = System.getenv("MARIADB_AVAILABLE");
        boolean mariaDBIsAvailable = mariaDBAvailable != null && mariaDBAvailable.equalsIgnoreCase("true");
        assumeTrue(mariaDBIsAvailable);
        assertEquals(0, Main.executeMain(new String[] { "--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                "--num-queries", "0", "mariadb" }));
    }

}
