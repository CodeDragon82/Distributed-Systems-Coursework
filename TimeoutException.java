/**
 * Exception is thrown when waiting from a packet responds times out.
 */
public class TimeoutException extends Exception {
    public TimeoutException(String _message) { super(_message); }
}
