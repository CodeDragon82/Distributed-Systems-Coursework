/**
 * Mutatable and volatile integer variable that can be passed by reference.
 */
public class Count {
    private volatile int value;

    public Count() {
        value = 0;
    }

    /**
     * Sets count to 0.
     */
    public void reset() {
        value = 0;
    }

    /**
     * Increments count by 1.
     */
    public void increment() {
        value++;
    }

    /**
     * Returns the count value.
     */
    public int getCount() {
        return value;
    }
}
