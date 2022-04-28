/**
 * Exception is throw when a received packet has the wrong format/data.
 */
public class PacketException extends Exception {
    public PacketException(String _message) { super(_message); }
}
