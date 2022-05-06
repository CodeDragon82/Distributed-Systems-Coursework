/**
 * Thrown when the controller fails to setpu a new incoming connection.
 */
public class ConnectionException extends Exception {
    public ConnectionException(String _message) { super(_message); }
}
