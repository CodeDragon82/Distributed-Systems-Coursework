public class IndexProperties {
    public int fileSize;

    public boolean storeInProcess;
    public int storeAcks;

    public boolean removeInProcess;
    public boolean removeAck;

    // Corrosponds to then dstore to attempt to load from.
    public int loadAttempt;

    public IndexProperties(int _fileSize) { 
        fileSize = _fileSize; 

        storeInProcess = false;
        storeAcks = 0;

        removeInProcess = false;
        removeAck = false;

        loadAttempt = 1;
    }
}