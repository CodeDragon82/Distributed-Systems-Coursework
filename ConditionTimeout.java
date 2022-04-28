import java.util.function.BooleanSupplier;

public class ConditionTimeout {
    public static void waitFor(BooleanSupplier condition, int timeout) throws TimeoutException {
        long startTime = System.currentTimeMillis();
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() - startTime < timeout)
                throw new TimeoutException("timed out while waiting for condition");
        }
    }
}
