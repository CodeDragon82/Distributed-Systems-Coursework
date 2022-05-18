import java.io.File;
import java.io.IOException;

public class Test2 {
    
    private static volatile Client client1;
    private static volatile Client client2;

    public static void main(String[] args) {
        Client client1 = new Client(5555, 1000, Logger.LoggingType.ON_TERMINAL_ONLY);
        Client client2 = new Client(5555, 1000, Logger.LoggingType.ON_TERMINAL_ONLY);

        new Thread() {
            @Override
            public void run() {
                try {
                    client1.connect();
                    client1.list();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                try {
                    client2.connect();
                    client2.list();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }
}
