import java.io.File;
import java.io.IOException;
import java.net.InetAddress;

public class DStore {
    private static ControllerListener controllerListener;
    private static DClientListener dClientListener;

    private static int port;
    private static int cport;
    private static int timeout;
    private static File fileFolder;

    public static void main(String[] args) {
        if (setupDStore(args)) {
            connectToController();
            setupClientListener();
        }
    }

    /**
     * Validate and assign parameters.
     */
    private static boolean setupDStore(String[] _args) {
        Message.process("setting up server", 0);

        boolean setupCorrectly = true;

        // validate the listening port specified.
        try {
            port = Integer.valueOf(_args[0]);

            if (!(port >= 1 && port <= 65535)) {
                Message.error("invalid listening port (must be between 1-65535)", 1);
    
                setupCorrectly = false;
            }
        } catch (NumberFormatException e) {
            Message.error("invalid listening port (must be integer)", 1);

            setupCorrectly = false;
        } catch (ArrayIndexOutOfBoundsException e) {
            Message.error("listening port not specified", 1);

            setupCorrectly = false;
        }

        // Validate the controller port specified.
        try {
            cport = Integer.valueOf(_args[1]);

            if (!(cport >= 1 && cport <= 65535)) {
                Message.error("invalid controller port (must be between 1-65535)", 1);
    
                setupCorrectly = false;
            }
        } catch (NumberFormatException e) {
            Message.error("invalid controller port (must be integer)", 1);

            setupCorrectly = false;
        } catch (ArrayIndexOutOfBoundsException e) {
            Message.error("controller port not specified", 1);

            setupCorrectly = false;
        }

        // Check for port conflict.
        if (port == cport) {
            Message.error("port conflict (listening port must be different from controller port)", 1);

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
            fileFolder = new File(_args[3]);
            if (!fileFolder.exists()) {
                Message.error("file folder doesn't exist", 1);

                setupCorrectly = false;
            }
        } catch (ArrayIndexOutOfBoundsException e) {
            Message.error("file folder not specified", 1);

            setupCorrectly = false;
        }

        if (setupCorrectly) Message.success("dstore setup complete", 0);
        else Message.failed("dstore setup failed", 0);

        return setupCorrectly;
    }

    /**
     * Setup a listener to send and receive messages to and from the controller.
     */
    private static void connectToController() {
        Message.process("setting up controller listener", 0);

        try {
            InetAddress ip = InetAddress.getLocalHost();
            controllerListener = new ControllerListener(ip, cport);
            controllerListener.setName("cont");
            controllerListener.start();

            Message.success("controller listener setup", 0);
        } catch (IOException e) {
            Message.failed("failed to setup controller listener", 0);
        }
    }

    /**
     * Setup a listener to send and receive messages to and from the client.
     */
    private static void setupClientListener() {
        Message.process("setting up client listener", 0);

        try {
            dClientListener = new DClientListener(port);
            dClientListener.setName("clnt");

            Message.success("client listener setup", 0);

            dClientListener.start();
        } catch (IOException e) {
            Message.failed("failed to setup client listener", 0);
        }
    }

    public static int getServerPort() { return port; }
    public static int getTimeout() { return timeout; }
    public static File getFileFolder() { return fileFolder; }

    public static DClientListener getDClientListener() { return dClientListener; }
    public static ControllerListener getControllerListener() { return controllerListener; }
}
