import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class Controller {
    private static int cPort;
    private static int replicationFactor;
    private static int timeout;
    private static int rebalancePeriod;

    private static List<ClientDstoreListener> dStoreListeners;

    public static void main(String[] args) throws IOException, IndexException {
        if (setupServer(args)) {
            RebalanceModule.scheduleRebalance();

            listenForConnections();
        }
    }

    private static boolean setupServer(String[] _args) {
        Message.process("setting up server", 0);

        boolean setupCorrectly = true;

        Index.setup();

        dStoreListeners = new ArrayList<ClientDstoreListener>();

        //// Validating arguments ////
        try {
            cPort = Integer.valueOf(_args[0]);

            if (!(cPort >= 1 && cPort <= 65535)) {
                Message.error("invalid port (must be between 1-65535)", 1);
    
                setupCorrectly = false;
            }
        } catch (NumberFormatException e) {
            Message.error("invalid port (must be integer)", 1);

            setupCorrectly = false;
        } catch (ArrayIndexOutOfBoundsException e) {
            Message.error("port not specified", 1);

            setupCorrectly = false;
        }

        try {
            replicationFactor = Integer.valueOf(_args[1]);

            if (!(replicationFactor > 0)) {
                Message.error("invalid replication factor (must be >0)", 1);

                setupCorrectly = false;
            }
        } catch (NumberFormatException e) {
            Message.error("invalid replication factor (must be integer)", 1);

            setupCorrectly = false;
        } catch (ArrayIndexOutOfBoundsException e) {
            Message.error("replication factor not specified", 1);

            setupCorrectly = false;
        }

        try {
            timeout = Integer.valueOf(_args[2]);
        } catch (NumberFormatException e) {
            Message.error("invalid timeout (must be integer)", 1);

            setupCorrectly = false;
        } catch (ArrayIndexOutOfBoundsException e) {
            Message.error("timeout not specified", 1);

            setupCorrectly = false;
        }

        try {
            rebalancePeriod = Integer.valueOf(_args[3]);

            if (!(rebalancePeriod > 0)) {
                Message.error("invalid rebalance period (must be >0)", 1);

                setupCorrectly = false;
            }
        } catch (NumberFormatException e) {
            Message.error("invalid rebalance period (must be integer)", 1);

            setupCorrectly = false;
        } catch (ArrayIndexOutOfBoundsException e) {
            Message.error("rebalance period not specified", 1);

            setupCorrectly = false;
        }
        //// .................... ////

        if (setupCorrectly) Message.success("server setup complete", 0);
        else Message.failed("server setup failed", 0);

        return setupCorrectly;
    }

    /**
     * Sets up the connection listener.
     * @throws IOException
     */
    private static void listenForConnections() throws IOException {
        try (ServerSocket serverSocket = new ServerSocket(cPort)) {
            Message.info("setup server socket", 0);

            while(true) {
                Socket newConnection = serverSocket.accept();

                Message.info("new connection established from " 
                    + newConnection.getInetAddress().getHostAddress() 
                    + ":" + newConnection.getPort(), 1);

                new ClientDstoreListener(newConnection).start();
            }
        }
    }

    /**
     * Returns true if atleast R dstores have joined.
     */
    public static boolean enoughDStores() { return dStoreListeners.size() >= replicationFactor; }

    public static void addDStoreListener(ClientDstoreListener _dStoreListener) { dStoreListeners.add(_dStoreListener); }

    public static List<ClientDstoreListener> getDStoreListeners() {
        
        // Remove unconnected dstores.
        checkDStoreConnections();

        return dStoreListeners; 
    }

    /**
     * Removes dstore listeners with failed connections.
     */
    private static void checkDStoreConnections() {
        Predicate<ClientDstoreListener> notConnected = ds -> !ds.isConnected();

        dStoreListeners.removeIf(notConnected);
    }

    public static int getReplicationFactor() { return replicationFactor; }
    public static int getTimeout() { return timeout; }
    public static int getRebalancePeriod() { return rebalancePeriod; }
}
