import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.IOException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Tests all the operations a client can perform on the controller/dstores.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OperationTests {
    private static final String TEST_FILE_NAME = "test.txt";
    private static final String TEST_FILE_DATA = "testing data";

    private static Client client;

    @BeforeAll
    public static void createClient() {
        client = new Client(5555, 1000, Logger.LoggingType.ON_TERMINAL_ONLY);

        connectClient();
    }

    private static void connectClient() {
        try {
            client.connect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    public static void disconnectClient() {
        try {
            client.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    @Order(1)
    public void testFirstList() {
        assertDoesNotThrow(() -> {
            assertEquals(0, client.list().length);
        });
    }

    @Test
    @Order(2)
    public void testInvalidLoad1() {
        assertThrows(FileDoesNotExistException.class, () -> {
            client.load(TEST_FILE_NAME);
        });
    }

    @Test
    @Order(3)
    public void testStore() {
        assertDoesNotThrow(() -> {
            client.store(TEST_FILE_NAME, TEST_FILE_DATA.getBytes());
        });
    }

    @Test
    @Order(4)
    public void testRepeatStore() {
        assertThrows(FileAlreadyExistsException.class, () -> {
            client.store(TEST_FILE_NAME, TEST_FILE_DATA.getBytes());
        });
    }

    /**
     * List stored lists.
     * 
     * This list should only contain one file, which is the file we just stored.
     */
    @Test
    @Order(5)
    public void testSecondList() {
        assertDoesNotThrow(() -> {
            String[] files = client.list();
            assertEquals(1, files.length);
            assertEquals(TEST_FILE_NAME, files[0]);
        });
    }

    /**
     * Load the stored file.
     */
    @Test
    @Order(6)
    public void testLoad() {
        assertDoesNotThrow(() -> {
            byte[] data = client.load(TEST_FILE_NAME);
            assertEquals(TEST_FILE_DATA, new String(data));
        });
    }

    /**
     * Remove a file that exists.
     */
    @Test
    @Order(7)
    public void testRemove() {
        assertDoesNotThrow(() -> {
            client.remove(TEST_FILE_NAME);
        });
    }

    /**
     * List stored files.
     * 
     * This should be an empty list after removing the only stored file.
     */
    @Test
    @Order(8)
    public void testThirdList() {
        assertDoesNotThrow(() -> {
            assertEquals(0, client.list().length);
        });
    }

    @Test
    @Order(9)
    public void testInvalidLoad2() {
        assertThrows(FileDoesNotExistException.class, () -> {
            client.load(TEST_FILE_NAME);
        });
    }
}
