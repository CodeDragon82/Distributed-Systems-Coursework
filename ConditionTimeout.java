public class ConditionTimeout {

    /**
     * Watches and waits for a flag to be set.
     * 
     * After a certain amount of time waiting, a timeout exception will be thrown.
     */
    public static void waitForFlag(Flag _flag, int _timeout) throws TimeoutException {
        long startTime = System.currentTimeMillis();
        while (!_flag.isSet()) {
            if (System.currentTimeMillis() - startTime > _timeout) {
                throw new TimeoutException("timed out while waiting for condition");
            }
        }
    }

    /**
     * Watches and waits for a count to have a target value.
     * 
     * After a certain amount of time waiting, a timeout exception will be thrown.
     */
    public static void waitForCount(Count _count, int _targetValue, int _timeout) throws TimeoutException{
        long startTime = System.currentTimeMillis();
        while (_count.getCount() != _targetValue) {
            if (System.currentTimeMillis() - startTime > _timeout) {
                throw new TimeoutException("timed out while waiting for condition");
            }
        }
    }
}
