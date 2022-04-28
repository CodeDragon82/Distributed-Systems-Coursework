import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public class RebalanceModule {

    private static Timer timer;

    private static boolean rebalancing;
    private static boolean rebalanceFlag;
    private static boolean listFlag;

    public static void scheduleRebalance() {
        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() { startRebalance(); }
        };

        int interval = Controller.getTimeout() * 1000;

        timer = new Timer("rebl");
        timer.schedule(timerTask, interval, interval);
    }

    public static void startRebalance() {
        try {
            rebalancing = true;
            rebalance();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            Message.error(e.getMessage(), 1);

            Message.failed("rebalancing operation failed", 0);
        } catch (RebalanceException e) {
            Message.error(e.getMessage(), 1);

            Message.failed("rebalancing operation failed", 0);
        } finally {
            rebalancing = false;
        }
    }

    /**
     * Performs the rebalancing operation.
     * 
     * @throws IOException
     * @throws TimeoutException
     * @throws RebalanceException
     */
    private static void rebalance() throws IOException, TimeoutException, RebalanceException {
        Message.process("starting rebalance", 0);

        RebalanceAlgorithm.setup(Controller.getDStoreListeners().size());

        for (DStoreListener dStoreListener : Controller.getDStoreListeners()) {
            listFlag = false;

            dStoreListener.sentToDStore("LIST");

            ConditionTimeout.waitFor(() -> listFlag, Controller.getTimeout());
        }

        RebalanceAlgorithm.calculate(Controller.getReplicationFactor());
            
        String[] packets = RebalanceAlgorithm.generate();
        for (int i = 0; i < packets.length; i++) {
            String packet = packets[i];

            rebalanceFlag = false;

            DStoreListener dStoreListener = Controller.getDStoreListeners().get(i);
            dStoreListener.sentToDStore(packet);

            ConditionTimeout.waitFor(() -> rebalanceFlag, Controller.getTimeout());
        }

        Message.success("rebalancing finished successfully", 0);
    }

    public static boolean isRebalancing() { return rebalancing; }

    public static void setListFlag() { listFlag = true; }

    public static void setRebalanceFlag() { rebalanceFlag = true; }

    /**
     * Map listed files to their corrsponding dstores (ports).
     */
    public static void addFileList(int _dstorePort, String[] _files) { 
        RebalanceAlgorithm.addFileStore(_dstorePort, _files);
    }
}
