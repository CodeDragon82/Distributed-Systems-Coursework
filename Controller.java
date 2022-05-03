import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class Controller {
    private static int cPort;
    private static int replicationFactor;
    private static int timeout;
    private static int rebalancePeriod;

    private static List<ClientListener> clientListeners;
    private static List<DStoreListener> dStoreListeners;

    public static void main(String[] args) throws IOException, IndexException {
        if (setupServer(args)) {
            listenForConnections();

            RebalanceModule.scheduleRebalance();

            Index.addFile("test1.txt", 11);
            Index.addFile("test2.txt", 7);
            Index.addFile("test3.txt", 7);

            commandShell();
        }
    }

    private static void commandShell() throws IOException {
        BufferedReader keyboard = new BufferedReader(new InputStreamReader(System.in));
    
        while (true) {
            String command = keyboard.readLine();
            System.out.println();

            if (command.equals("help")) displayHelp();
            else if (command.equals("connections")) displayConnections();
        }
    }

    private static void displayHelp() {
        System.out.println("help");
    }

    private static void displayConnections() {
        System.out.println("DStores\n-------");

        getDStoreListeners().stream().forEach(ds -> System.out.println("DStore\t" 
                                                                + "connection on port: "
                                                                + ds.getSocket().getPort()
                                                                + "\tlistening to clients on port: "
                                                                + ds.getClientPort()));

    }

    private static boolean setupServer(String[] _args) {
        Message.process("setting up server", 0);

        boolean setupCorrectly = true;

        Index.setup();

        clientListeners = new ArrayList<ClientListener>();
        dStoreListeners = new ArrayList<DStoreListener>();

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
     */
    private static void listenForConnections() {
        Message.process("setting up connection listener", 0);

        try {
            ConnectionListener connectionListener = new ConnectionListener(cPort);
            connectionListener.setName("conn"); // Name thread.

            Message.success("connection listener setup complete", 0);

            connectionListener.start();
        } catch (IOException e) {
            Message.error("failed to setup connection listener", 0);
        }
    }

    /**
     * Returns true if atleast R dstores have joined.
     */
    public static boolean enoughDStores() { return dStoreListeners.size() >= replicationFactor; }

    public static void setClientListener(ClientListener _clientListener) { clientListeners.add(_clientListener); }
    public static void addDStoreListener(DStoreListener _dStoreListener) { dStoreListeners.add(_dStoreListener); }

    public static List<ClientListener> getClientListener() { 

        // Remove unconnected client.
        checkClientConnections();

        return clientListeners;
    }

    public static List<DStoreListener> getDStoreListeners() {
        
        // Remove unconnected dstores.
        checkDStoreConnections();

        return dStoreListeners; 
    }

    /**
     * Removes client listeners with failed connections.
     */
    private static void checkClientConnections() {
        Predicate<ClientListener> notConnected = cl -> !cl.isConnected();

        clientListeners.removeIf(notConnected);
    }

    /**
     * Removes dstore listeners with failed connections.
     */
    private static void checkDStoreConnections() {
        Predicate<DStoreListener> notConnected = ds -> !ds.isConnected();

        dStoreListeners.removeIf(notConnected);
    }

    public static int getReplicationFactor() { return replicationFactor; }
    public static int getTimeout() { return timeout; }
    public static int getRebalancePeriod() { return rebalancePeriod; }
}
