import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

public class TestDStore {
    public static void main(String[] args) throws Exception {
        Socket socket = new Socket(InetAddress.getLocalHost(), 4444);
        socket.setSoTimeout(1000);

        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        BufferedReader keyboard = new BufferedReader(new InputStreamReader(System.in));
        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
    
        out.println("");
        while (true) {
            System.out.print("> ");
            String command = keyboard.readLine();

            out.println(command);

            try {
                String response = in.readLine();
                System.out.println(response);
            } catch (Exception e) {
                System.out.println("timeout error");
            }
        }
    }
}
