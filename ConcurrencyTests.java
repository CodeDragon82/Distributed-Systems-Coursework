import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.IOException;

import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;

public class ConcurrencyTests {
    private static final String TEST_FILE_NAME = "test.txt";
    private static final String TEST_FILE_DATA = "testing data";

    private static final int CLIENT_COUNT = 10;

    public static Client[] clients;

    @BeforeAll
    public static void setUp() throws IOException {
        createClients();
        connectClients();
        storeFile();
    }

    private static void createClients() {
        clients = new Client[CLIENT_COUNT];

        for (int i = 0; i < CLIENT_COUNT; i++) {
            clients[i] = new Client(5555, 1000, Logger.LoggingType.ON_TERMINAL_ONLY);
        }
    }

    private static void connectClients() throws IOException {
        for (int i = 0; i < CLIENT_COUNT; i++) {
            clients[i].connect();
        }
    }

    private static void storeFile() throws IOException {
        clients[0].store(TEST_FILE_NAME, TEST_FILE_DATA.getBytes());
    }

    @AfterAll
    public static void cleanUp() throws IOException {
        disconnectClients();
    }

    private static void disconnectClients() throws IOException {
        for (int i = 0; i < CLIENT_COUNT; i++) {
            clients[i].disconnect();
        }
    }

    @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
    private class Tests {
        private Client client;

        public Tests(Client _client) {
            client = _client;
        }

        @Test
        @Order(1)
        public void testConcurrentList() {
            assertDoesNotThrow(() -> assertEquals(1, client.list()));
        }


        @Test
        @Order(2)
        public void testConcurrentLoad() {
            assertDoesNotThrow(() -> assertEquals(TEST_FILE_DATA.getBytes(), client.load(TEST_FILE_NAME)));
        }
    }
}
