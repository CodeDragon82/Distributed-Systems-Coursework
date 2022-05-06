import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Tests the rebalancing algorithm.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RebalanceTests {
    private static final String FILE_1 = "file1.txt";
    private static final String FILE_2 = "file2.txt";
    private static final String FILE_3 = "file3.txt";

    private static final String[] FILE_SET_1 = {FILE_1};
    private static final String[] FILE_SET_2 = {FILE_2};
    private static final String[] FILE_SET_3 = {FILE_3};
    private static final String[] FILE_SET_1_2 = {FILE_1, FILE_2};
    private static final String[] FILE_SET_1_3 = {FILE_1, FILE_3};
    private static final String[] FILE_SET_2_3 = {FILE_2, FILE_3};
    private static final String[] FILE_SET_1_2_3 = {FILE_1, FILE_2, FILE_3};

    @Test
    @Order(1)
    public void testNoFiles() {
        int storeCount = 3;
        int replicationFactor = 3;

        RebalanceAlgorithm.setup(storeCount);

        for (int i = 1; i <= storeCount; i++) {
            RebalanceAlgorithm.addFileStore(i * 1000, new String[0]);
        }

        assertDoesNotThrow(() -> RebalanceAlgorithm.calculate(replicationFactor));

        for (String message : RebalanceAlgorithm.generate()) {
            assertEquals("REBALANCE 0 0", message);
        }
    }

    @Test void testRemoveFiles() {
        int storeCount = 3;
        int replicationFactor = 1;

        RebalanceAlgorithm.setup(storeCount);

        for (int i = 1; i <= storeCount; i++) {
            RebalanceAlgorithm.addFileStore(i * 1000, FILE_SET_1_2_3);
        }

        assertDoesNotThrow(() -> RebalanceAlgorithm.calculate(replicationFactor));

        String[] messages = RebalanceAlgorithm.generate();
        assertEquals("REBALANCE 0 2 " + FILE_2 + " " + FILE_3, messages[0]);
        assertEquals("REBALANCE 0 2 " + FILE_1 + " " + FILE_3, messages[1]);
        assertEquals("REBALANCE 0 2 " + FILE_1 + " " + FILE_2, messages[2]);
    }
}
