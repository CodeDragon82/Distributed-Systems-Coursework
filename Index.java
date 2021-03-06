import java.util.HashMap;
import java.util.function.Predicate;

public class Index {

    private static HashMap<String, IndexProperties> files;

    public static void setup() {
        files = new HashMap<String, IndexProperties>();
    }

    public static void clear() {
        files.clear();
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

    /**
     * Returns an array of files that stored in the index that 
     * are not in "process of store" or "process of remove".
     */
    public static String[] listFiles() { 
        Predicate<String> condition = file -> !files.get(file).removeInProcess.isSet()
                                           && !files.get(file).storeInProcess.isSet();
        String[] availableFiles = files.keySet().stream().filter(condition).toArray(String[]::new);

        return availableFiles;
    }

    /**
     * Return true if a filename exists in the file index.
     */
    public static boolean fileExists(String _fileName) { 
        return files.keySet().stream().anyMatch(s -> s.equals(_fileName)); 
    }

    public static int fileSize(String _fileName) { return files.get(_fileName).fileSize; }



    //// STORING ////

    public static boolean isFileBeingStored(String _fileName) { 
        return files.get(_fileName).storeInProcess.isSet(); 
    }

    /**
     * Set whether or not the controller is in the process of storing the file.
     */
    public static void storingFile(String _fileName, boolean _storeInProcess) { 
        if (_storeInProcess) files.get(_fileName).storeInProcess.set();
        else files.get(_fileName).storeInProcess.reset();
    }

    public static void resetStoreAcks(String _fileName) {
        files.get(_fileName).storeAcks.reset();
    }

    public static void incrementStoreAcks(String _fileName) {
        files.get(_fileName).storeAcks.increment();
    }

    public static Count getStoreAcks(String _fileName) {
        return files.get(_fileName).storeAcks;
    }



    //// REMOVING ////

    /**
     * Return true if the file is in the process of being removed.
     */
    public static boolean isFileBeingRemoved(String _fileName) { 
        return files.get(_fileName).removeInProcess.isSet(); 
    }

    /**
     * Set whether or not hte controller is in the process of removing the file.
     */
    public static void removingFile(String _fileName, boolean _removeInProcess) {
        if (_removeInProcess) files.get(_fileName).removeInProcess.set();
        else files.get(_fileName).removeInProcess.reset();
    }

    public static void setRemoveAck(String _fileName, boolean _removeAck) {
        if (_removeAck) files.get(_fileName).removeAck.set();
        else files.get(_fileName).removeAck.reset();
    }

    public static Flag getRemoveAck(String _fileName) { 
        return files.get(_fileName).removeAck;
    }
}
