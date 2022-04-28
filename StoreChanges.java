import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class StoreChanges {
    private HashMap<String, LinkedList<Integer>> filesToSend;
    private LinkedList<String> filesToRemove;

    public StoreChanges() {
        filesToSend = new HashMap<String, LinkedList<Integer>>();
        filesToRemove = new LinkedList<String>();
    }

    /**
     * Record a file that needs to be sent from this file store.
     * @param _file
     * @param _port
     */
    public void sendFile(String _file, Integer _port) {
        if (!filesToSend.containsKey(_file))
            filesToSend.put(_file, new LinkedList<Integer>());

        filesToSend.get(_file).add(_port);
    }

    /**
     * Record a file that needs to be removed from this file store.
     * @param _file
     */
    public void removeFile(String _file) {
        filesToRemove.add(_file);
    }

    /**
     * Generate a rebalance message from the list of file changes.
     * @return
     */
    public String generateMessage() {
        String message = "REBALANCE";
        
        message += " " + filesToSend.size();
        for (Map.Entry<String, LinkedList<Integer>> fileToSend : filesToSend.entrySet()) {
            message += " " + fileToSend.getKey();
            message += " " + fileToSend.getValue().size();
            for (Integer port : fileToSend.getValue())
                message += " " + port;
        }

        message += " " + filesToRemove.size();
        for (String file : filesToRemove)
            message += " " + file;

        return message;
    }
}
