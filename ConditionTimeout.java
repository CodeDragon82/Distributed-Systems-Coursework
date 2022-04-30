import java.util.function.BooleanSupplier;

public class ConditionTimeout {
    public static void waitForFlag(Flag _flag, int _timeout) throws TimeoutException {
        long startTime = System.currentTimeMillis();
        while (!_flag.isSet()) {
            if (System.currentTimeMillis() - startTime > _timeout) {
                throw new TimeoutException("timed out while waiting for condition");
            }
        }
    }

    public static void waitForCondition(BooleanSupplier _condition, int _timeout) throws TimeoutException{
        long startTime = System.currentTimeMillis();
        while (!_condition.getAsBoolean()) {
            if (System.currentTimeMillis() - startTime > _timeout) {
                throw new TimeoutException("timed out while waiting for condition");
            }
        }
    }
}
