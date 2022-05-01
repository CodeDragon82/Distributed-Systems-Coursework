import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

public class TestLoad {
    public static void main(String[] args) throws UnknownHostException, IOException, InterruptedException {
        int controllerPort = Integer.parseInt(args[0]);
        String fileName = args[1];

        Socket controllerSocket = new Socket(InetAddress.getLocalHost(), controllerPort);
        controllerSocket.setSoTimeout(1000);
        BufferedReader in = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
        PrintWriter out = new PrintWriter(controllerSocket.getOutputStream(), true);

        out.println("LOAD " + fileName);

        String packet = in.readLine();
        System.out.println(packet);

        controllerSocket.close();

        String[] packetParts = packet.split(" ");
        int dstorePort = Integer.parseInt(packetParts[1]);
        int fileSize = Integer.parseInt(packetParts[2]);

        Socket dstoreSocket = new Socket(InetAddress.getLocalHost(), dstorePort);
        dstoreSocket.setSoTimeout(1000);
        in = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));
        out = new PrintWriter(dstoreSocket.getOutputStream(), true);

        out.println("LOAD_DATA " + fileName);

        byte[] data = dstoreSocket.getInputStream().readNBytes(fileSize);
        System.out.println(new String(data));
    }
}
