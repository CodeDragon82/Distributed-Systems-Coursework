/**
 * Mutatable and volatile boolean variable that can be passed by reference.
 */
public class Flag {
    private volatile boolean value;

    public Flag() {
        value = false;
    }

    public void reset() {
        value = false;
    }

    public void set() {
        value = true;
    }

    /**
     * Returns the boolean value of the flag.
     */
    public boolean isSet() {
        return value;
    }
}
