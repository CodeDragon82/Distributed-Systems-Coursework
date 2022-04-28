import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

/**
 * Used by the dstore to listen to send and receive messages to and from the client.
 */
public class DClientListener extends Thread {
    private static ServerSocket connectSocket;

    private static Socket clientSocket;

    private static BufferedReader in;
    private static PrintWriter out;

    public DClientListener(int _port) throws IOException {
        try {
            Message.process("opening server socket", 1);
            connectSocket = new ServerSocket(_port);
            Message.success("server socket opened on port " + _port, 1);
        } catch (IOException e) {
            Message.error("failed to open server socket on port " + _port, 1);
            
            throw e;
        }
    }

    @Override
    public void run() {
        while (true) {

            if (clientSocket == null) listenForClientConnection();
            else if (clientSocket.isClosed()) {
                clientSocket = null;
                in = null;
                out = null;
            } else listenForClientMessage();
        }
    }

    private void listenForClientConnection() {
        Message.info("listening for client connections ...", 0);

        try {
            clientSocket = connectSocket.accept();

            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            out = new PrintWriter(clientSocket.getOutputStream(), true);

            // Set reading timeout.
            clientSocket.setSoTimeout(DStore.getTimeout());

            Message.info("client connection from: " + clientSocket.getInetAddress(), 0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void respond(String _packet) throws IOException {
        out.println(_packet);

        Message.info("sent response: " + _packet, 1);
    }

    private void sentData(String _data) throws IOException {
        clientSocket.getOutputStream().write(_data.getBytes());

        Message.info("sent data: " + _data, 1);
    }

    private void listenForClientMessage() {
        try {
            String packet = in.readLine();
            System.out.println();
            processPacket(packet);
        } catch (SocketTimeoutException e) {
            System.out.print("+");
        } catch (IOException e) {
            Message.info("client connection closed", 0);

            try {
                in.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
            out.close();
            clientSocket = null;
        }
    }


    private void processPacket(String _packet) throws IOException {
        Message.process("processing packet from client: " + _packet, 0);

        if (_packet == null) {
            Message.error("received null packet", 1);

            Message.failed("couldn't process packet", 0);

            throw new IOException();
        }

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

        try {
            if (command.equals("STORE")) processStore(arguments);
            else if (command.equals("LOAD_DATA")) processLoadData(arguments);
            else if (command.equals("REBALANCE_STORE")) processRebalanceStore(arguments);
            else throw new PacketException("incorrect/missing command");

            Message.success("packet processed correctly", 0);
        } catch (PacketException e) {
            Message.error(e.getMessage(), 1);

            Message.failed("couldn't process packet", 0);
        } catch (TimeoutException e) {
            Message.error(e.getMessage(), 1);

            Message.failed("processing packet timeout", 0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Processes STORE packet sent by client.
     */
    private void processStore(String[] _arguments) throws IOException, PacketException, TimeoutException {
        Message.info("STORE request", 1);

        // Validate arguments.
        if (_arguments.length != 2) {
            throw new PacketException("STORE command must have 2 arguments");
        }

        int fileSize;
        try {
            fileSize = Integer.valueOf(_arguments[1]);
        } catch (NumberFormatException e) {
            throw new PacketException("file size argument must be an integer");
        }

        if (fileSize < 0) throw new PacketException("file size must be positive");

        // Check if file exists.
        String fileName = _arguments[0];
        File file = new File(DStore.getFileFolder(), fileName);
        if (file.exists()) throw new PacketException("file already exist");

        respond("ACK");

        Message.info("reading file content ...", 1);
        // Receive file contents from client.
        String fileContent = "";
        try {
            byte[] fileBytes = clientSocket.getInputStream().readNBytes(fileSize);
            fileContent = new String(fileBytes);
        } catch (SocketTimeoutException e) {
            throw new TimeoutException("timed out while reading file content");
        } catch (IOException e) {
            throw e;
        }

        // Write file content to new file.
        FileWriter fileWriter = new FileWriter(file);
        fileWriter.write(fileContent);
        fileWriter.close();

        DStore.getControllerListener().respondToController("STORE_ACK " + fileName);
    }


    
    //// LOAD DATA OPERATION ////

    /**
     * Processes LOAD_DATA packet sent by client.
     */
    private void processLoadData(String[] _arguments) throws IOException, PacketException {
        Message.info("LOAD_DATA request", 1);

        if (validateLoadData(_arguments)) {
            String fileName = _arguments[0];
            File file = new File(DStore.getFileFolder(), fileName);

            performLoadData(file);
        }
    }

    private boolean validateLoadData(String[] _arguments) throws PacketException {
        if (_arguments.length != 1) {
            throw new PacketException("LOAD_DATA command must have 1 argument!");
        }

        String fileName = _arguments[0];
        File file = new File(DStore.getFileFolder(), fileName);

        if (!file.exists()) throw new PacketException(fileName + " doesn't exist!");

        return true;
    }

    private void performLoadData(File _file) throws IOException {

        // Read contents of file.
        FileReader fileReader = new FileReader(_file);
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        String line = bufferedReader.readLine();
        String fileContent = line;
        while ((line = bufferedReader.readLine()) != null) fileContent += "\n" + line;
        bufferedReader.close();
        fileReader.close();

        sentData(fileContent);
    }

    //// REBALANCE STORE OPERATION ////

    /**
     * Processes REBALANCE_STORE packet sent by another dstore.
     */
    private void processRebalanceStore(String[] _arguments) throws PacketException, IOException {
        Message.info("REBALANCE_STORE request", 1);

        if (validateRebalanceStore(_arguments)) {
            String fileName = _arguments[0];
            int fileSize = Integer.valueOf(_arguments[1]);

            performRebalanceStore(fileName, fileSize);
        }
    }

    private boolean validateRebalanceStore(String[] _arguments) throws PacketException {
        if (_arguments.length != 2) {
            throw new PacketException("REBALANCE_STORE command must have 2 arguments");
        }

        int fileSize;
        try { 
            fileSize = Integer.valueOf(_arguments[1]); 
        } catch (NumberFormatException e) { 
            throw new PacketException("file size must be an integer");
        }

        if (fileSize < 0) throw new PacketException("file size must be positive");

        return true;
    }

    private void performRebalanceStore(String _fileName, int _fileSize) throws IOException {

        // Send acknowledgement to other dstore.
        respond("ACK");

        // Receive file content from the other dstore.
        byte[] fileBytes = clientSocket.getInputStream().readNBytes(_fileSize);
        String fileContent = new String(fileBytes);

        // Create a file and write the file content to it.
        File file = new File(DStore.getFileFolder(), _fileName);
        FileWriter fileWriter = new FileWriter(file);
        fileWriter.write(fileContent);
        fileWriter.close();
    }
}