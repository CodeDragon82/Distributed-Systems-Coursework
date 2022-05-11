public class IndexProperties {
    public int fileSize;

    /**
     * Flag is set if the file is currently in the process of being stored.
     */
    public Flag storeInProcess;

    /**
     * Counts the number of STORE acknowledgements during a STORE operation.
     */
    public Count storeAcks;

    public Flag removeInProcess;
    public Flag removeAck;

    public IndexProperties(int _fileSize) {
        fileSize = _fileSize; 

        storeInProcess = new Flag();
        storeAcks = new Count();

        removeInProcess = new Flag();
        removeAck = new Flag();
    }
}