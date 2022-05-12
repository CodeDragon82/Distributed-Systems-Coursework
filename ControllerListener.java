import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Used by the dstore to send and receive messages to and from the controller.
 */
public class ControllerListener extends Thread {
    private InetAddress controllerAddress;
    private int controllerPort;

    private Socket controllerSocket;

    private BufferedReader in;
    private PrintWriter out;

    public ControllerListener(InetAddress _address, int _port) throws IOException {
        controllerAddress = _address;
        controllerPort = _port;

        connect();
    }

    private void connect() throws IOException {
        // Setup controller connection.
        try {
            controllerSocket = new Socket(controllerAddress, controllerPort);

            Message.info("connected to controller at " + controllerAddress + ":" + controllerPort, 1);
        } catch (IOException e) {
            Message.error("couldn't connect to controller at " + controllerAddress + ":" + controllerPort, 1);

            throw e;
        }

        // Setup controller input and output streams.
        try {
            in = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
            out = new PrintWriter(controllerSocket.getOutputStream(), true);

            // Set reading timeout.
            controllerSocket.setSoTimeout(Dstore.getTimeout());

            Message.info("input and output streams setup", 1);
        } catch (IOException e) {
            Message.error("failed to setup input and output streams", 1);

            throw e;
        }

        out.println("JOIN");
        Message.info("sent JOIN message", 1);

        out.println(Dstore.getServerPort());
        Message.info("sent server port", 1);
    }

    @Override
    public void run() {
        while (isConnected()) {
            try {
                String packet = in.readLine();
                processPacket(packet);
            } catch (SocketTimeoutException e) {
                // Read timeout.
                // Normal behaviour.
            } catch (PacketException e) {
                Message.error(e.getMessage(), 1);

                Message.failed("failed to process packet", 0);
            } catch (SocketException e) {
                Message.info("controller conneciton closed", 0);

                Message.process("reconnecting to controller ...", 0);

                try { 
                    connect(); 

                    Message.success("reconnected to controller", 0);
                } catch (IOException e1) { 
                    Message.failed("failed to reconnect to controller", 0);
                    
                    break;
                }
            } catch (IOException e) {
                Message.error("controller listener crashed: " + e.getMessage(), 0);
                break;
            }
        }
    }

    /**
     * Checks if the client socket is connected and open.
     */
    public boolean isConnected() {
        if (controllerSocket == null) return false;
        if (!controllerSocket.isConnected()) return false;
        if (controllerSocket.isClosed()) return false;

        return true;
    }

    public void respondToController(String _packet) throws IOException {
        out.println(_packet);

        Message.info("sent response: " + _packet, 1);
    }

    private void processPacket(String _packet) throws PacketException, IOException {
        Message.process("processing packet from controller: " + _packet, 0);

        if (_packet == null) throw new IOException();

        String[] packetContent = _packet.split(" ");
        String command = "";
        String[] arguments = new String[0];

        if (packetContent.length > 0) {
            command = packetContent[0];
            int arugmentCount = packetContent.length - 1;
            arguments = new String[arugmentCount];
            for (int i = 0; i < arugmentCount; i++) 
                arguments[i] = packetContent[i + 1];
        }

        if (command.equals("LIST")) processList(arguments);
        else if (command.equals("REMOVE")) processRemove(arguments);
        else if (command.equals("REBALANCE")) processRebalance(arguments);
        else throw new PacketException("incorrect/missing command");

        Message.success("packet processed correctly", 0);
    }

    /**
     * Processing list request from controller to dstore.
     * 
     * This happens when the controller is rebalancing the network.
     * @throws IOException
     */
    private void processList(String[] _arguments) throws PacketException, IOException {
        Message.info("LIST request", 1);

        if (_arguments.length != 0 ) {
            Message.error("invalid arguments (must have no arguments)", 1);

            throw new PacketException("LIST command must have no arguments");
        }

        File[] files = Dstore.getFileFolder().listFiles();
        String fileNames = "";
        for (File file : files) fileNames += " " + file.getName();

        respondToController("LIST" + fileNames);
    }

    private void processRemove(String[] _arguments) throws PacketException, IOException {
        Message.info("REMOVE request", 1);

        if (_arguments.length != 1) {
            throw new PacketException("REMOVE packet must contain 1 arugment");
        }

        String fileName = _arguments[0];
        File fileToRemove = new File(Dstore.getFileFolder(), fileName);

        if (fileToRemove.exists()) fileToRemove.delete();
        else {
            respondToController("ERROR_FILE_DOES_NOT_EXIST");
            return;
        }

        respondToController("REMOVE_ACK " + fileName);
    }



    //// REBALANCING OPERATIONS ////

    private void processRebalance(String[] _arguments) throws UnknownHostException, IOException, PacketException {
        Message.info("REBALANCE request", 1);

        if (_arguments.length < 2) {
            throw new PacketException("REBALANCE packet must have atleast 2 arguments");
        }

        int currentArgument = 0;

        // Extract files that need to be sent.
        int numberOfFilesToSend = Integer.valueOf(_arguments[currentArgument]);
        currentArgument++;
        for (int currentFileToSend = 0; currentFileToSend < numberOfFilesToSend; currentFileToSend++) {
            String fileName = _arguments[currentArgument];
            currentArgument++;

            int numberOfDstores = Integer.valueOf(_arguments[currentArgument]);
            currentArgument++;
            for (int j = 0; j < numberOfDstores; j++) {
                int dstorePort = Integer.valueOf(_arguments[currentArgument]);
                currentArgument++;

                sendFile(fileName, dstorePort);
            }
        }

        // Extract files that need to be removed.
        int numberOfFilesToRemove = Integer.valueOf(_arguments[currentArgument]);
        currentArgument++;
        for (int currentFileToRemove = 0; currentFileToRemove < numberOfFilesToRemove; currentFileToRemove++) {
            String fileName = _arguments[currentArgument];
            currentArgument++;

            removeFile(fileName);
        }

        respondToController("REBALANCE_COMPLETE");
    }

    /**
     * Sends a file to another dstore.
     * 
     * Happens during rebalancing.
     */
    private void sendFile(String _fileName, int _dstorePort) throws UnknownHostException, IOException, PacketException {
        Message.info("sending file to " + _dstorePort + ": " + _fileName, 1);

        Socket socket = new Socket(InetAddress.getLocalHost(), _dstorePort);
        // socket.setSoTimeout(DStore.getTimeout());

        // Send REBALANCE_STORE packet to dstore.
        PrintWriter printWriter = new PrintWriter(socket.getOutputStream(), true);
        int fileSize = (int) Files.size(Paths.get(Dstore.getFileFolder().getAbsolutePath(), _fileName));
        String outputPacket = "REBALANCE_STORE " + _fileName + " " + fileSize;
        printWriter.println(outputPacket);

        Message.info("sent packet: " + outputPacket, 2);

        // Wait to receive ACK packet from dstore.
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        String inPacket = bufferedReader.readLine();
        if (!inPacket.equals("ACK")) 
            throw new PacketException("unexpected response from dstore");

        Message.info("received acknowledgement", 2);

        // Read contents of file.
        File file = new File(Dstore.getFileFolder(), _fileName);
        BufferedReader fileReader = new BufferedReader(new FileReader(file));
        String line = fileReader.readLine();
        String fileContent = line;
        while ((line = fileReader.readLine()) != null) fileContent += "\n" + line;
        fileReader.close();

        Message.info("read file contents", 2);

        // Send file content to other dstore.
        socket.getOutputStream().write(fileContent.getBytes());

        Message.info("sent file contents", 2);
    }

    /**
     * Remove a file from this dstore.
     * 
     * Happens during rebalancing.
     */
    private void removeFile(String _fileName) {
        Message.info("removing file: " + _fileName, 1);

        File file = new File(Dstore.getFileFolder(), _fileName);
        file.delete();
        
        Message.info("removed file", 2);
    }

    //// ////
}