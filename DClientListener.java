import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

/**
 * Used by the dstore to listen to send and receive 
 * messages to and from the client and other dstores.
 */
public class DClientListener extends Thread {
    private static ServerSocket serverSocket;

    private static Socket socket;

    public DClientListener(int _port) throws IOException {
        try {
            serverSocket = new ServerSocket(_port);
            Message.info("server socket opened on port " + _port, 1);
        } catch (IOException e) {
            Message.error("failed to open server socket on port " + _port, 1);
            
            throw e;
        }
    }

    @Override
    public void run() {
        while (true) {
            Message.info("waiting for connection ...", 0);

            try {
                socket = serverSocket.accept();
                socket.setSoTimeout(Dstore.getTimeout());

                Message.info("connection from: " + socket.getInetAddress(), 0);

                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String packet = in.readLine();
                
                processPacket(packet);
            } catch (PacketException e) {
                Message.error(e.getMessage(), 1);

                Message.failed("failed to process packet", 0);
            } catch (TimeoutException e) {
                Message.error(e.getMessage(), 1);

                Message.failed("timed out while processing packet", 0);
            } catch (IOException e) {
                e.printStackTrace();
            } 
        }
    }

    /**
     * Send packet to connected node.
     */
    private void respond(String _packet) throws IOException {
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        out.println(_packet);

        Message.info("sent response: " + _packet, 1);
    }

    /**
     * Send raw byte data to connected node.
     */
    private void sentData(byte[] _data) throws IOException {
        socket.getOutputStream().write(_data);

        Message.info("sent data: " + _data, 1);
    }

    private void processPacket(String _packet) throws IOException, PacketException, TimeoutException {
        Message.process("processing packet: " + _packet, 0);

        if (_packet == null) throw new PacketException("couldn't process null packet");

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
        else if (command.equals("LOAD_DATA")) processLoadData(arguments);
        else if (command.equals("REBALANCE_STORE")) processRebalanceStore(arguments);
        else throw new PacketException("incorrect/missing command");

        Message.success("packet processed correctly", 0);
    }


    
    //// STORE OPERATION ////

    /**
     * Processes STORE packet sent by client.
     */
    private void processStore(String[] _arguments) throws IOException, PacketException, TimeoutException {
        Message.info("STORE request", 1);

        if (validateStore(_arguments)) {
            String fileName = _arguments[0];
            int fileSize = Integer.valueOf(_arguments[1]);

            performStore(fileName, fileSize);
        }

    }

    private boolean validateStore(String[] _arguments) throws PacketException {
        // Validate arguments.
        if (_arguments.length != 2) {
            throw new PacketException("STORE command must have 2 arguments");
        }

        // Check if file exists.
        String fileName = _arguments[0];
        File file = new File(Dstore.getFileFolder(), fileName);
        if (file.exists()) throw new PacketException("file already exist");

        int fileSize;
        try {
            fileSize = Integer.valueOf(_arguments[1]);
        } catch (NumberFormatException e) {
            throw new PacketException("file size argument must be an integer");
        }

        if (fileSize < 0) throw new PacketException("file size must be positive");

        return true;
    }

    private void performStore(String _fileName, int _fileSize) throws TimeoutException, IOException {
        respond("ACK");

        Message.info("reading file content ...", 1);
        // Receive file contents from client.
        byte[] fileBytes;
        try {
            fileBytes = socket.getInputStream().readNBytes(_fileSize);
        } catch (SocketTimeoutException e) {
            throw new TimeoutException("timed out while reading file content");
        } catch (IOException e) {
            throw e;
        }

        // Write file content to new file.
        File file = new File(Dstore.getFileFolder(), _fileName);
        FileOutputStream fileWriter = new FileOutputStream(file);
        fileWriter.write(fileBytes);
        fileWriter.close();

        Dstore.getControllerListener().respondToController("STORE_ACK " + _fileName);
    }

    

    //// LOAD DATA OPERATION ////

    /**
     * Processes LOAD_DATA packet sent by client.
     */
    private void processLoadData(String[] _arguments) throws IOException, PacketException {
        Message.info("LOAD_DATA request", 1);

        if (validateLoadData(_arguments)) {
            String fileName = _arguments[0];
            File file = new File(Dstore.getFileFolder(), fileName);

            performLoadData(file);
        }
    }

    private boolean validateLoadData(String[] _arguments) throws PacketException {
        if (_arguments.length != 1) {
            throw new PacketException("LOAD_DATA command must have 1 argument!");
        }

        String fileName = _arguments[0];
        File file = new File(Dstore.getFileFolder(), fileName);

        if (!file.exists()) throw new PacketException(fileName + " doesn't exist!");

        return true;
    }

    private void performLoadData(File _file) throws IOException {
        FileInputStream fileReader = new FileInputStream(_file);
        byte[] fileBytes = fileReader.readAllBytes();
        fileReader.close();

        sentData(fileBytes);
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

        Message.info("sending acknowledgement", 1);

        // Receive file content from the other dstore.
        byte[] fileBytes = socket.getInputStream().readNBytes(_fileSize);
        String fileContent = new String(fileBytes);

        Message.info("received file data: " + _fileName, 1);

        // Create a file and write the file content to it.
        File file = new File(Dstore.getFileFolder(), _fileName);
        FileWriter fileWriter = new FileWriter(file);
        fileWriter.write(fileContent);
        fileWriter.close();

        Message.info("wrote file data to new file: " + _fileName, 1);
    }
}
