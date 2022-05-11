import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.function.Predicate;

public class RebalanceAlgorithm {

    private static int storeCount;

    // Current set of files stored on each dstore.
    private static LinkedList<Set<String>> currentFileDistribution;

    // New set of files stored on each dstore.
    private static LinkedList<Set<String>> newFileDistribution;

    // Maps files to the dstore number that they are stored in.
    private static HashMap<String, LinkedList<Integer>> fileMap;

    // Maps dstore number to dstore port.
    private static LinkedList<Integer> ports;

    // Store the required changes to each file store to rebalance.
    private static StoreChanges[] distributionChanges;



    //// SET UP ////

    public static void setup(int _storeCount) {
        storeCount = _storeCount;

        currentFileDistribution = new LinkedList<Set<String>>();
        newFileDistribution = new LinkedList<Set<String>>();
        fileMap = new HashMap<String, LinkedList<Integer>>();
        ports = new LinkedList<Integer>();

        distributionChanges = new StoreChanges[storeCount];
        for (int i = 0; i < storeCount; i++)
            distributionChanges[i] = new StoreChanges();
    }



    //// RECORDING CURRENT DISTRIBUTION ////

    /**
     * Add set of files from a dstore to the current file distribution.
     * 
     * Each file is also added to the file map, which maps files to dstores.
     * @throws RebalanceException
     */
    public static void addFileStore(Integer port, String[] files) throws RebalanceException {
        if (ports.stream().anyMatch(_port -> _port == port ))
            throw new RebalanceException("store with the port " + port + " has already been added");

        if (currentFileDistribution.size() >= storeCount)
            throw new RebalanceException("can't add more than " + storeCount + " file stores");

        ports.add(port);

        Set<String> fileStore = new LinkedHashSet<String>();

        for (String file : files) {
            fileStore.add(file);

            if (fileMap.get(file) == null)
                fileMap.put(file, new LinkedList<Integer>());

            fileMap.get(file).add(currentFileDistribution.size());
        }

        currentFileDistribution.add(fileStore);
    }




    //// CALCULATE NEW FILE DISTRIBUTION ////

    /**
     * Calculate the new set of distributed files from each dstore.
     * @throws RebalanceException
     */
    public static void calculate(int _replicationFactor) throws RebalanceException {
        if (_replicationFactor > currentFileDistribution.size())
            throw new RebalanceException("replication factor can't be greater than the number of file stores");
        
        if (storeCount != currentFileDistribution.size())
            throw new RebalanceException("not enough file stores have been added");

        String[] files = listFiles();
        redistributeFiles(files, _replicationFactor);

        validateNewDistribution(files.length,  _replicationFactor);

        calculateChanges();
    }

    /**
     * Collects a list of all files distributed through out all dstores.
     */
    private static String[] listFiles() {
        Set<String> fileSet = new LinkedHashSet<String>();

        for (Set<String> dstoreFiles : currentFileDistribution)
            for (String file : dstoreFiles)
                fileSet.add(file);

        String[] files = new String[fileSet.size()];
        int i = 0;
        for (String file : fileSet) {
            files[i] = file;
            i++;
        }

        return files;
    }

    /**
     * Create a new distribution for the list files based on the replicaiton factor.
     */
    private static void redistributeFiles(String[] _files, int _replicationFactor) {
        for (int i = 0; i < currentFileDistribution.size(); i++)
            newFileDistribution.add(new LinkedHashSet<String>());

        for (String file : _files) {
            for (int i = 0; i < _replicationFactor; i++) {
                // Add the file to the file store with the least number of files.
                newFileDistribution.stream().min(Comparator.comparingInt(Set::size)).get().add(file);
            }
        }
    }

    /**
     * Valid that the files have been evenly distributed between the stores.
     */
    private static void validateNewDistribution(int _fileCount, int _replicationFactor) throws RebalanceException {
        Predicate<Set<String>> invalidFileCount = s -> !validFileCount(s, _replicationFactor, _fileCount, currentFileDistribution.size());
        if (newFileDistribution.stream().anyMatch(invalidFileCount))
            throw new RebalanceException("invalid file count in file store");
    }

    /**
     * Validate the number of files in a store is allowed.
     */
    private static boolean validFileCount(Set<String> _fileStore, int _R, int _F, int _N) {
        int minFileCount = (int) Math.floor(((double)_R * _F) / _N);
        int maxFileCount = (int) Math.ceil(((double)_R * _F) / _N);

        if (_fileStore.size() < minFileCount) return false;
        if (_fileStore.size() > maxFileCount) return false;

        return true;
    }

    /**
     * Calculate and record the changes that need to be made to
     * each file store in the current file distribution.
     */
    private static void calculateChanges() {

        // For each file store in the file distribution.
        for (int i = 0; i < currentFileDistribution.size(); i++) {

            Set<String> currentFileStore = currentFileDistribution.get(i);
            Set<String> newFileStore = newFileDistribution.get(i);

            // Calculate the files to remove from the file store.
            Set<String> filesToRemove = new LinkedHashSet<String>();
            filesToRemove.addAll(currentFileStore);
            filesToRemove.removeAll(newFileStore);

            for (String file : filesToRemove)
                distributionChanges[i].removeFile(file);

            // Calculate the files to add to the file store.
            Set<String> filesToAdd = new LinkedHashSet<String>();
            filesToAdd.addAll(newFileStore);
            filesToAdd.removeAll(currentFileStore);

            for (String file : filesToAdd) {
                Integer from = fileMap.get(file).getFirst();
                Integer toPort = ports.get(i);

                distributionChanges[from].sendFile(file, toPort);
            }
        }
    }



    //// GENERATE REBALANCE MESSAGES ////

    /**
     * Generate rebalance messages to send to each dstore.
     * 
     * This is generated by comparing the orginal file 
     * distribution with the new file distribution.
     */
    public static String[] generate() {
        String[] messages = new String[currentFileDistribution.size()];

        for (int i = 0; i < currentFileDistribution.size(); i++)
            messages[i] = distributionChanges[i].generateMessage();

        return messages;
    }
}
