import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;

public class DstoreListener extends Thread {
    private Socket dStoreSocket;

    private BufferedReader in;
    private PrintWriter out;

    private int clientPort;

    public DstoreListener(Socket _dStoreSocket) throws IOException, NumberFormatException {
        dStoreSocket = _dStoreSocket;
        dStoreSocket.setSoTimeout(Controller.getTimeout());
        in = new BufferedReader(new InputStreamReader(dStoreSocket.getInputStream()));
        out = new PrintWriter(dStoreSocket.getOutputStream(), true);

        try {
            clientPort = Integer.valueOf(in.readLine());

            Message.info("dstore is listening for client messages on port " + clientPort, 1);
        } catch (IOException e) {
            Message.error("failed to get client listening port", 1);

            cleanUp();

            throw e;
        } catch (NumberFormatException e) {
            Message.error("client listening port is invalid", 1);

            cleanUp();

            throw e;
        }
    }

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
            } catch (IOException e) {
                cleanUp();
            }
        }

        Message.info("dstore connection closed", 0);
    }

    private void cleanUp() {
        out.close();

        try {
            in.close();
            dStoreSocket.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }

        dStoreSocket = null;
    }

    /**
     * Checks if the client socket is connected and open.
     */
    public boolean isConnected() {
        if (dStoreSocket == null) return false;
        if (!dStoreSocket.isConnected()) return false;
        if (dStoreSocket.isClosed()) return false;

        return true;
    }



    //// PROCESS PACKETS ////

    private void processPacket(String _packet) throws PacketException {
        Message.process("processing packet from dstore: " + _packet, 0);

        if (_packet == null) throw new PacketException("can't process null packet");

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
        else if (command.equals("REMOVE_ACK")) processRemoveAck(arguments);
        else if (command.equals("STORE_ACK")) processStoreAck(arguments);
        else if (command.equals("REBALANCE_COMPLETE")) processRebalanceComplete(arguments);
        else if (command.equals("ERROR_NOT_ENOUGH_DSTORES")) processError(command);
        else if (command.equals("ERROR_FILE_DOES_NOT_EXIST")) processError(command);
        else throw new PacketException("Incorrect/missing command!");

        Message.success("packet processed correctly", 0);
    }

    /**
     * Processes LIST response from dstore to controller.
     * 
     * Happens during the rebalancing operation.
     */
    private void processList(String[] _arguments) throws PacketException {
        Message.info("LIST response", 1);

        Message.info("giving files to rebalancing operation", 1);
        RebalanceModule.addFileList(clientPort, _arguments);
        RebalanceModule.setListAck();
    }

    private void processRemoveAck(String[] _arguments) throws PacketException {
        Message.info("REMOVE_ACK response", 1);

        if (_arguments.length != 1) {
            Message.error("invalid arguments (must have 1 argument)", 1);

            throw new PacketException("REMOVE_ACK command must have 1 arguments");
        }

        String fileName = _arguments[0];
        
        Index.setRemoveAck(fileName, true);
    }

    private void processStoreAck(String[] _arguments) throws PacketException {
        Message.info("STORE_ACK response", 1);

        if (_arguments.length != 1) {
            Message.error("invalid argument (must have 1 arugment)", 1);

            throw new PacketException("STORE_ACK command must have 1 argument");
        }

        String fileName = _arguments[0];

        Index.incrementStoreAcks(fileName);
    }

    private void processRebalanceComplete(String[] _arguments) throws PacketException {
        Message.info("REBALANCE_COMPLETE response", 1);

        if (_arguments.length > 0) {
            Message.error("invalid argument (must have no arugments)", 1);

            throw new PacketException("REBALANCE_COMPLETE command must have no arugments");
        }

        RebalanceModule.setRebalanceAck();
    }

    private void processError(String _error) {
        Message.info(_error, 1);
    }



    //// OUT BOUND ////

    public void sentToDStore(String _packet) throws IOException {
        Message.info("sending to dstore: " + _packet, 1);

        out.println(_packet);    
    }



    //// PROPERTIES ////

    public int getClientPort() { return clientPort; }
}
