public class NullPacketException extends Exception {
    public NullPacketException(String _source) { super("received a null packet from " + _source ); }
}
