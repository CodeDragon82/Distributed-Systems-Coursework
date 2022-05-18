import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;

public class RebalanceModule {

    private static Timer timer;

    private static Flag rebalancing;

    private static Flag rebalanceAck;
    private static Flag listAck;

    public static void scheduleRebalance() {
        rebalancing = new Flag();

        rebalanceAck = new Flag();
        listAck = new Flag();

        timer = new Timer("rebl");
        timer.schedule(new TimerTask() { 
            @Override
            public void run() { 
                if (Controller.enoughDStores()) startRebalance();
                else Message.info("waiting for more dstores to join", 0);
            }
        }, Controller.getRebalancePeriod(), Controller.getRebalancePeriod());
    }

    /**
     * Starts a rebalancing operation if rebalancing isn't already running.
     */
    private static void startRebalance() {
        if (rebalancing.isSet()) return;
        
        try {
            rebalancing.set();
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
            rebalancing.reset();
        }
    }

    public static void rebalanceEarly() {
        timer.cancel();
        timer.purge();

        timer = new Timer("rebl");
        timer.schedule(new TimerTask() { 
            @Override
            public void run() { 
                if (Controller.enoughDStores()) startRebalance();
                else Message.info("waiting for more dstores to join", 0);
            }
        }, 0, Controller.getRebalancePeriod());
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
        for (ClientDstoreListener dStoreListener : Controller.getDStoreListeners()) {
            listAck.reset();

            dStoreListener.respond("LIST");

            // Wait for LIST acknowledgement.
            ConditionTimeout.waitForFlag(listAck, Controller.getTimeout());
        }

        Message.info("running rebalance algorithm", 1);
        RebalanceAlgorithm.calculate(Controller.getReplicationFactor());
        
        Message.info("sending rebalance packets to dstores", 1);
        String[] packets = RebalanceAlgorithm.generate();
        for (int i = 0; i < packets.length; i++) {
            String packet = packets[i];

            rebalanceAck.reset();

            ClientDstoreListener dStoreListener = Controller.getDStoreListeners().get(i);
            dStoreListener.respond(packet);

            // Wait for REBALANCE acknowledgement.
            ConditionTimeout.waitForFlag(rebalanceAck, Controller.getTimeout());
        }

        Message.success("rebalancing finished successfully", 0);
    }

    public static boolean isRebalancing() { 
        return rebalancing.isSet(); 
    }

    /**
     * LIST acknowledge is set when the dstore listener receives a LIST response.
     */
    public static void setListAck() { 
        listAck.set();
    }

    public static void setRebalanceAck() { 
        rebalanceAck.set();
    }

    /**
     * Map listed files to their corrsponding dstores (ports).
     */
    public static void addFileList(int _dstorePort, String[] _files) { 
        try {
            RebalanceAlgorithm.addFileStore(_dstorePort, _files);
        } catch (RebalanceException e) {
            Message.error(e.getMessage(), 1);
        }
    }
}
