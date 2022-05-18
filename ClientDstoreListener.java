import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ClientDstoreListener extends Thread {

    private Socket socket;

    private BufferedReader in;
    private PrintWriter out;

    /**
     * Current loading attempt.
     * 
     * Corrosponds to the next dstore to load from.
     */
    public Count loadAttempt;

    private int dstorePort;

    public ClientDstoreListener(Socket _socket) throws IOException {
        socket = _socket;

        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        out = new PrintWriter(socket.getOutputStream(), true);

        loadAttempt = new Count();
    }

    @Override
    public void run() {
        while(isConnected()) handleNextPacket();
    }

    /**
     * Checks if the client socket is connected and open.
     */
    public boolean isConnected() {
        if (socket == null) return false;
        if (!socket.isConnected()) return false;
        if (socket.isClosed()) return false;

        return true;
    }

    private void closeConnection() {
        out.close();
        try {
            in.close();
            socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        socket = null;

        Controller.getDStoreListeners().remove(this);
    }

    private boolean isDstore() {
        return Controller.getDStoreListeners().contains(this);
    }


    //// PROCESS PACKETS ////

    private void handleNextPacket() {
        try {
            if (!RebalanceModule.isRebalancing() || isDstore()) {
                String packet = in.readLine();
                Message.process("processing next packet: " + packet, 0);
                
                processPacket(packet);

                Message.success("packet processed correctly", 0);
            }
        } catch (PacketException e) {
            Message.error(e.getMessage(), 1);

            Message.failed("failed to process packet", 0);
        } catch (TimeoutException e) {
            Message.error(e.getMessage(), 1);

            Message.failed("processing packet timed out", 0);
        } catch (NullPacketException e) {
            Message.error(e.getMessage(), 1);

            closeConnection();

            Message.info("connection closed", 0);
        } catch (IOException e) {
            closeConnection();

            Message.info("connection closed", 0);
        } 
    }

    private void processPacket(String _packet) throws PacketException, IOException, TimeoutException, NullPacketException {
        if (_packet == null) throw new NullPacketException("client");

        String[] packetContent = _packet.split(" ");
        String command = "";
        String[] arguments = new String[0];


        // Extract command and arguments from packet.
        if (packetContent.length > 0) {
            command = packetContent[0];
            int arugmentCount = packetContent.length - 1;
            arguments = new String[arugmentCount];
            for (int i = 0; i < arugmentCount; i++) 
                arguments[i] = packetContent[i + 1];
        }

        if (command.equals("STORE")) processStore(arguments);
        else if (command.equals("LOAD")) processLoad(arguments);
        else if (command.equals("RELOAD")) processReload(arguments);
        else if (command.equals("REMOVE")) processRemove(arguments);
        else if (command.equals("LIST")) {
            if (isDstore()) processDstoreList(arguments);
            else processClientList(arguments);
        } else if (command.equals("REMOVE_ACK")) processRemoveAck(arguments);
        else if (command.equals("STORE_ACK")) processStoreAck(arguments);
        else if (command.equals("REBALANCE_COMPLETE")) processRebalanceComplete(arguments);
        else if (command.equals("JOIN")) processJoin(arguments);
        else if (command.equals("ERROR_NOT_ENOUGH_DSTORES")) processError(command);
        else if (command.equals("ERROR_FILE_DOES_NOT_EXIST")) processError(command);
        else throw new PacketException("incorrect/missing command");
    }



    //// STORE OPERATION ////

    /**
     * Process STORE request from client.
     */
    private void processStore(String[] _arguments) throws PacketException, IOException, TimeoutException {
        Message.info("STORE request", 1);

        if (validateStore(_arguments)) performStore(_arguments[0], Integer.valueOf(_arguments[1]));
    }

    private boolean validateStore(String[] _arguments) throws PacketException, IOException {
        if (_arguments.length != 2) {
            throw new PacketException("LOAD command must have 1 argument!");
        }

        // Check if file size argument is a positive integer.
        try {
            int fileSize = Integer.valueOf(_arguments[1]);
            if (fileSize < 0) throw new PacketException("file size must be positive");
        } catch (NumberFormatException e) {
            throw new PacketException("file size must be an integer");
        }

        // Check if R dstores are available.
        if (!Controller.enoughDStores()) {
            Message.info("not enough dstores have joined", 1);

            respond("ERROR_NOT_ENOUGH_DSTORES");
            return false;
        }

        String fileName = _arguments[0];

        // Check if file exists.
        if (Index.fileExists(fileName)) {
            Message.info("file already exists", 1);

            respond("ERROR_FILE_ALREADY_EXISTS");
            return false;
        }

        return true;
    }

    private void performStore(String _fileName, int _fileSize) throws IOException, PacketException, TimeoutException {
        // Add file to the file index.
        try {
            Index.addFile(_fileName, _fileSize);
        } catch (IndexException e) {
            throw new PacketException(e.getMessage());
        }

        // Set "storing file in process" to true.
        Index.storingFile(_fileName, true);

        // Collect port number from each dstore listener.
        String dStorePorts = "";
        for (ClientDstoreListener dStoreListener : Controller.getDStoreListeners())
            dStorePorts += " " + dStoreListener.getClientPort();

        respond("STORE_TO" + dStorePorts);

        // Wait for number of received store acks to match the number of dstores.
        try {
            ConditionTimeout.waitForCount(Index.getStoreAcks(_fileName), 
                                          Controller.getDStoreListeners().size(), 
                                          Controller.getTimeout());
        } catch (TimeoutException e) {

            // If waiting times out, then remove file from the index.
            Index.removeFile(_fileName);

            throw e;
        }

        // Set "storing file in process" to false.
        Index.storingFile(_fileName, false);

        respond("STORE_COMPLETE");
    }



    //// LOAD OPERATION ////

    /**
     * Process LOAD request from client.
     */
    private void processLoad(String[] _arguments) throws PacketException, IOException {
        Message.info("LOAD request", 1);

        if (validateLoad(_arguments)) performLoad(_arguments[0]);
    }

    /**
     * Process RELOAD request from client.
     */
    private void processReload(String[] _arguments) throws PacketException, IOException {
        Message.info("RELOAD request", 1);

        if (validateLoad(_arguments)) performReload(_arguments[0]);
    }

    private boolean validateLoad(String[] _arguments) throws IOException, PacketException {
        // Validate arguments.
        if (_arguments.length != 1) {
            throw new PacketException("LOAD/RELOAD command must have 1 argument!");
        }

        // Check if R dstores are available.
        if (!Controller.enoughDStores()) {
            Message.info("not enough dstores have joined", 1);

            respond("ERROR_NOT_ENOUGH_DSTORES");
            return false;
        }

        String fileName = _arguments[0];

        // Check if file exists.
        if (!Index.fileExists(fileName)) {
            Message.info("file doesn't exist", 1);

            respond("ERROR_FILE_DOES_NOT_EXIST");
            return false;
        }

        // Check if file is in "process of being stored".
        if (Index.isFileBeingStored(fileName)) {
            Message.info("file is in the process of being stored", 1);

            respond("ERROR_FILE_DOES_NOT_EXIST");
            return false;
        }

        // Check if file is in "process of being removed".
        if (Index.isFileBeingRemoved(fileName)) {
            Message.info("file is in the process of being removed", 1);

            respond("ERROR_FILE_DOES_NOT_EXIST");
            return false;
        }

        return true;
    }

    private void performLoad(String _fileName) throws IOException {
        loadAttempt.reset();

        int dStorePort = Controller.getDStoreListeners().get(0).getClientPort();
        int fileSize = Index.fileSize(_fileName);

        respond("LOAD_FROM " + dStorePort + " " + fileSize);
    }

    private void performReload(String _fileName) throws IOException {
        loadAttempt.increment();

        if (loadAttempt.getCount() >= Controller.getDStoreListeners().size()) {
            respond("ERROR_LOAD");
            return;
        }

        int dStorePort = Controller.getDStoreListeners().get(loadAttempt.getCount()).getClientPort();
        int fileSize = Index.fileSize(_fileName);

        respond("LOAD_FROM " + dStorePort + " " + fileSize);
    }



    //// REMOVE OPERATION ////

    /**
     * Process REMOVE request from client.
     */
    private void processRemove(String[] _arguments) throws PacketException, IOException, TimeoutException{
        Message.info("REMOVE request", 1);

        if (validateRemove(_arguments)) performRemove(_arguments[0]);
    }

    private boolean validateRemove(String[] _arguments) throws IOException, PacketException {
        // Check if one arugment is supplied.
        if (_arguments.length != 1) {
            throw new PacketException("REMOVE command must have 1 argument!");
        }

        // Check if R dstores are available.
        if (!Controller.enoughDStores()) {
            Message.info("no dstores available", 1);

            respond("ERROR_NOT_ENOUGH_DSTORES");
            return false;
        }

        String fileName = _arguments[0];
        
        // Check if the file exists in the controller's file index.
        if (!Index.fileExists(fileName)) {
            Message.info(fileName + " doesn't exist in the index", 1);

            respond("ERROR_FILE_DOES_NOT_EXIST");
            return false;
        }

        // Check if the file is in the process of being removed.
        if (Index.isFileBeingRemoved(fileName)) {
            Message.info(fileName + " is in the process of being removed", 1);

            respond("ERROR_FILE_DOES_NOT_EXIST");
            return false;
        }

        // Check if the file is in the process of being stored.
        if (Index.isFileBeingStored(fileName)) {
            Message.info("file is in the process of being stored", 1);

            respond("ERROR_FILE_DOES_NOT_EXIST");
            return false;
        }

        return true;
    }
    
    private void performRemove(String _fileName) throws IOException, TimeoutException {

        // Set "removing file in process" to true.
        Index.removingFile(_fileName, true);

        // Instruct each dstore to remove the target file.
        for (ClientDstoreListener dstoreListener : Controller.getDStoreListeners()) {
            try {
                // Sends a packet to the dstore to remove the file and waits for an ACK message.
                removeFile(dstoreListener, _fileName);

                Message.info("removed file from dstore " + dstoreListener.getClientPort(), 1);
            } catch (TimeoutException e) {
                Message.error("failed to remove file from dstore " + dstoreListener.getClientPort(), 1);
            
                throw e;
            }
        }

        // Remove target file from file index.
        Index.removeFile(_fileName);

        respond("REMOVE_COMPLETE");
    }

    /**
     * Manages the removal of a file from a dstore.
     */
    private void removeFile(ClientDstoreListener _dstoreListener, String _fileName) throws IOException, TimeoutException {
        // Set the remove ACK flag to false.
        Index.setRemoveAck(_fileName, false);

        // Send REMOVE instruction to dstore.
        _dstoreListener.respond("REMOVE " + _fileName);

        // Wait to receive remove ACK from dstore.
        ConditionTimeout.waitForFlag(Index.getRemoveAck(_fileName), Controller.getTimeout());

        Message.info("removed " + _fileName + " from dstore ", 1);

        // Reset the remove ACK flag to false.
        Index.setRemoveAck(_fileName, false);
    }



    //// (client) LIST OPERATION ////

    /**
     * Process LIST request from client.
     */
    private void processClientList(String[] _arguments) throws PacketException, IOException {
        Message.info("LIST request", 1);

        if (validateClientList(_arguments)) performClientList();
    }

    private boolean validateClientList(String[] _arguments) throws IOException, PacketException {
        if (_arguments.length != 0) {
            throw new PacketException("LIST command contains arguments!");
        }

        // Check if R dstores are available.
        if (!Controller.enoughDStores()) {
            respond("ERROR_NOT_ENOUGH_DSTORES");
            return false;
        }

        return true;
    }

    private void performClientList() throws IOException {
        Message.info("performing LIST operation", 1);
        
        // Collect file names into one string.
        String files = String.join(" ", Index.listFiles());

        // Send list of files to client.
        respond("LIST " + files);
    }



    //// (dstore) LIST OPERATION ////

    private void processDstoreList(String[] _arguments) throws PacketException {
        Message.info("LIST response", 1);

        Message.info("giving files to rebalancing operation", 1);
        RebalanceModule.addFileList(dstorePort, _arguments);
        RebalanceModule.setListAck();
    }



    //// REMOVE ACKNOWLEDGEMENT ////

    private void processRemoveAck(String[] _arguments) throws PacketException {
        Message.info("REMOVE_ACK response", 1);

        if (_arguments.length != 1) {
            Message.error("invalid arguments (must have 1 argument)", 1);

            throw new PacketException("REMOVE_ACK command must have 1 arguments");
        }

        String fileName = _arguments[0];
        
        Index.setRemoveAck(fileName, true);
    }



    //// STORE ACKNOWLEDGEMENT ////

    private void processStoreAck(String[] _arguments) throws PacketException {
        Message.info("STORE_ACK response", 1);

        if (_arguments.length != 1) {
            Message.error("invalid argument (must have 1 arugment)", 1);

            throw new PacketException("STORE_ACK command must have 1 argument");
        }

        String fileName = _arguments[0];

        Index.incrementStoreAcks(fileName);
    }



    //// REBALANCE ACKNOWLEDGEMENT ////

    private void processRebalanceComplete(String[] _arguments) throws PacketException {
        Message.info("REBALANCE_COMPLETE response", 1);

        if (_arguments.length > 0) {
            throw new PacketException("REBALANCE_COMPLETE command must have no arugments");
        }

        RebalanceModule.setRebalanceAck();
    }



    //// JOIN OPERATION ////

    private void processJoin(String[] _arguments) throws PacketException {
        Message.info("JOIN request", 1);

        if (_arguments.length > 0) {
            throw new PacketException("JOIN command must have no arugments");
        }

        try {
            dstorePort = Integer.valueOf(in.readLine());
        } catch (NumberFormatException | IOException e) {
            throw new PacketException("failed to get dstore port");
        }

        Controller.addDStoreListener(this);

        Message.info("new dstore has joined", 1);

        RebalanceModule.rebalanceEarly();
    }



    //// PROCESS ERROR PACKET ////

    private void processError(String _error) {
        Message.info(_error, 1);
    }



    //// RESPONDING ////

    public void respond(String _packet) {
        out.println(_packet);

        Message.info("sent response: " + _packet, 1);
    }



    //// DSTORE PORT ////

    public int getClientPort() { return dstorePort; }
}
