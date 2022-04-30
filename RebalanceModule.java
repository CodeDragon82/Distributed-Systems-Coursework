import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public class RebalanceModule {

    private static Timer timer;

    private static boolean rebalancing;

    private static Flag rebalanceFlag;
    private static Flag listFlag;

    public static void scheduleRebalance() {
        rebalanceFlag = new Flag();
        listFlag = new Flag();

        TimerTask timerTask = new TimerTask() {
            @Override
            public void run() { 
                if (Controller.enoughDStores()) startRebalance();
            }
        };

        int interval = Controller.getRebalancePeriod() * 1000;

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

        Message.info("setting up algorithm", 1);
        RebalanceAlgorithm.setup(Controller.getDStoreListeners().size());

        Message.info("getting file listings from dstores", 1);
        for (DStoreListener dStoreListener : Controller.getDStoreListeners()) {
            listFlag.reset();

            dStoreListener.sentToDStore("LIST");

            ConditionTimeout.waitForFlag(listFlag, Controller.getTimeout());
        }

        Message.info("running rebalance algorithm", 1);
        RebalanceAlgorithm.calculate(Controller.getReplicationFactor());
        
        Message.info("sending rebalance packets to dstores", 1);
        String[] packets = RebalanceAlgorithm.generate();
        for (int i = 0; i < packets.length; i++) {
            String packet = packets[i];

            rebalanceFlag.reset();

            DStoreListener dStoreListener = Controller.getDStoreListeners().get(i);
            dStoreListener.sentToDStore(packet);

            ConditionTimeout.waitForFlag(rebalanceFlag, Controller.getTimeout());
        }

        Message.success("rebalancing finished successfully", 0);
    }

    public static boolean isRebalancing() { return rebalancing; }

    public static void setListFlag() { 
        listFlag.set();
        System.out.println(listFlag.isSet());
    }

    public static void setRebalanceFlag() { rebalanceFlag.set(); }

    /**
     * Map listed files to their corrsponding dstores (ports).
     */
    public static void addFileList(int _dstorePort, String[] _files) { 
        RebalanceAlgorithm.addFileStore(_dstorePort, _files);
    }
}
