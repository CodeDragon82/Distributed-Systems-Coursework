import java.util.HashMap;
import java.util.Set;

public class Index {

    private static HashMap<String, IndexProperties> files;

    public static void setup() {
        files = new HashMap<String, IndexProperties>();
    }

    /**
     * Add file to file index.
     * @throws IndexException
     */
    public static void addFile(String _fileName, int _fileSize) throws IndexException {
        if (_fileSize < 0) throw new IndexException("file size must be positive");

        files.put(_fileName, new IndexProperties(_fileSize));
    }

    public static void removeFile(String _fileName) {
        files.remove(_fileName);
    }

    public static Set<String> listFiles() { return files.keySet(); }

    /**
     * Return true if a filename exists in the file index.
     */
    public static boolean fileExists(String _fileName) { 
        return files.keySet().stream().anyMatch(s -> s.equals(_fileName)); 
    }

    public static int fileSize(String _fileName) { return files.get(_fileName).fileSize; }

    //// STORING ////

    public static boolean isFileBeingStored(String _fileName) { 
        return files.get(_fileName).storeInProcess; 
    }

    /**
     * Set whether or not the controller is in the process of storing the file.
     */
    public static void storingFile(String _fileName, boolean _storeInProcess) { 
        files.get(_fileName).storeInProcess = _storeInProcess; 
    }

    public static void resetStoreAcks(String _fileName) {
        files.get(_fileName).storeAcks = 0;
    }

    public static void incrementStoreAcks(String _fileName) {
        files.get(_fileName).storeAcks++;
    }

    public static int getStoreAcks(String _fileName) {
        return files.get(_fileName).storeAcks;
    }

    //// REMOVING ////

    public static boolean isFileBeingRemoved(String _fileName) { 
        return files.get(_fileName).removeInProcess; 
    }

    /**
     * Set whether or not hte controller is in the process of removing the file.
     */
    public static void removingFile(String _fileName, boolean _removeInProcess) {
        files.get(_fileName).removeInProcess = _removeInProcess;
    }

    public static void setRemoveAck(String _fileName, boolean _removeAck) {
        files.get(_fileName).removeAck = _removeAck;
    }

    public static boolean getRemoveAck(String _fileName) { 
        return files.get(_fileName).removeAck;
    }

    //// LOADING ////

    public static void resetLoadAttempt(String _fileName) {
        files.get(_fileName).loadAttempt = 1;
    }

    public static void incrementLoadAttempt(String _fileName) {
        files.get(_fileName).loadAttempt++;
    }

    public static int getLoadAttempt(String _fileName) {
        return files.get(_fileName).loadAttempt;
    }
}
