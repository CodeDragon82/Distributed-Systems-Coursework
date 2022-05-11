import java.io.BufferedReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import java.io.InputStreamReader;

/**
 * Deals with new connections to the server.
 * 
 * If the new connection sends a 'JOIN' message,
 * then it is a new dstore.
 */
public class ConnectionListener extends Thread {
    private static ServerSocket serverSocket;

    public ConnectionListener(int _port) throws IOException {
        try {
            serverSocket = new ServerSocket(_port);

            Message.info("server socket opened on port " + _port, 1);
        } catch (IOException e) {
            Message.error("failed to open server socket on port " + _port, 1);
            
            throw e;
        }
    }

    public void run() {
        while (!serverSocket.isClosed()) {
            Message.info("waiting for new connections", 0);

            Socket newConnection = new Socket();
            try {
                newConnection = serverSocket.accept();

                Message.process("processing new connection", 0);
                Message.info("new connection established from " 
                            + newConnection.getInetAddress().getHostAddress() 
                            + ":" + newConnection.getPort(), 1);

                String firstPacket = readFirstPacket(newConnection);
                if (firstPacket.equals("JOIN")) connectionFromDStore(newConnection);
                else connectionFromClient(newConnection, firstPacket);

                Message.success("connection setup successfully", 0);
                
            } catch (ConnectionException e) {
                Message.error(e.getMessage(), 1);

                try {
                    newConnection.close();

                    Message.info("closed new connection", 1);
                } catch (IOException e1) {
                    e1.printStackTrace();

                    Message.error("couldn't close new connection", 1);
                }

                Message.failed("connection setup failed", 0);
            } catch (IOException e) {
                Message.error("server socket died", 0);

                try {
                    serverSocket.close();

                    Message.info("server socket closed", 0);
                } catch (IOException e1) {
                    Message.error("couldn't close server socket", 0);
                }
            }
        }
    }

    /**
     * Reads the first packet from the new connection.
     */
    private String readFirstPacket(Socket _socket) throws ConnectionException {
        String firstPacket = "";
        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(_socket.getInputStream()));
            firstPacket = in.readLine();
        } catch (IOException e) {
            throw new ConnectionException("couldn't read first packet from new connection");
        }

        return firstPacket;
    }
    
    /**
     * Sets up a client listener for the newly connected client.
     */
    private void connectionFromClient(Socket _clientSocket, String _firstPacket) throws ConnectionException {
        Message.info("connection is from a client", 1);

        try {
            ClientListener clientListener = new ClientListener(_clientSocket, _firstPacket);
            clientListener.setName("clnt");
            Controller.setClientListener(clientListener);
            clientListener.start();

            Message.info("set client listener", 1);

        } catch (IOException e) {
            throw new ConnectionException("failed to create client listener");
        }
    }

    /**
     * Sets up a dstore listener for the newly connected dstore.
     */
    private void connectionFromDStore(Socket _dStoreSocket) throws ConnectionException {
        Message.info("connection is from a dstore", 1);

        try {
            DstoreListener dStoreListener = new DstoreListener(_dStoreSocket);
            dStoreListener.setName("dstr");
            Controller.addDStoreListener(dStoreListener);
            dStoreListener.start();

            Message.info("set dstore listener", 1);

            // Start rebalancing operation when a dstore joins.
            RebalanceModule.startRebalance();
        } catch (IOException e) {
            throw new ConnectionException("failed to create dstore listener");
        }
    }
}
