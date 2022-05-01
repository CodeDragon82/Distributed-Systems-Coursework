public class IndexProperties {
    public int fileSize;

    public Flag storeInProcess;

    /**
     * Counts the number of STORE acknowledgements during a STORE operation.
     */
    public Count storeAcks;

    public Flag removeInProcess;
    public Flag removeAck;

    /**
     * Corrosponds to then dstore to attempt to load from.
     */
    public Count loadAttempt;

    public IndexProperties(int _fileSize) {
        fileSize = _fileSize; 

        storeInProcess.reset();;
        storeAcks.reset();

        removeInProcess.reset();;
        removeAck.reset();

        loadAttempt.reset();
    }
}