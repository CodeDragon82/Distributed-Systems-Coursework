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
    private static ServerSocket connectSocket;

    public ConnectionListener(int _port) throws IOException {
        try {
            connectSocket = new ServerSocket(_port);

            Message.info("server socket opened on port " + _port, 1);
        } catch (IOException e) {
            Message.error("failed to open server socket on port " + _port, 1);
            
            throw e;
        }
    }

    public void run() {
        while (true) {
            Message.info("waiting for new connections", 0);

            try {
                Socket newConnection = connectSocket.accept();

                Message.process("processing new connection", 0);
                Message.info("new connection established from " 
                            + newConnection.getInetAddress().getHostAddress() 
                            + ":" + newConnection.getPort(), 1);

                BufferedReader in = new BufferedReader(new InputStreamReader(newConnection.getInputStream()));
                
                String firstPacket = in.readLine();
                if (firstPacket.equals("JOIN")) connectionFromDStore(newConnection);
                else connectionFromClient(newConnection, firstPacket);
                
            } catch (IOException e) {
                Message.info("connection listener crashed (creating new socket)", 1);
            }
        }
    }
    
    private void connectionFromClient(Socket _clientSocket, String _firstPacket) {
        Message.info("connection is from a client", 1);

        try {
            ClientListener clientListener = new ClientListener(_clientSocket, _firstPacket);
            clientListener.setName("clnt");
            Controller.setClientListener(clientListener);
            clientListener.start();

            Message.info("set client listener", 1);
            Message.success("", 0);

        } catch (Exception e) {
            Message.error("failed to create client listener", 1);
            e.printStackTrace();
            Message.failed("", 0);
        }
    }

    private void connectionFromDStore(Socket _dStoreSocket) {
        Message.info("connection is from a dstore", 1);

        try {
            DStoreListener dStoreListener = new DStoreListener(_dStoreSocket);
            dStoreListener.setName("dstr");
            Controller.addDStoreListener(dStoreListener);
            dStoreListener.start();

            Message.info("set dstore listener", 1);
            Message.success("", 0);

            // Start rebalancing operation when a dstore joins.
            RebalanceModule.startRebalance();
        } catch (Exception e) {
            Message.error("failed to create dstore listener", 1);
            Message.failed("", 0);
        }
    }
}
