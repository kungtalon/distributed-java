import cluster.management.LeaderElection;
import cluster.management.OnElectionAction;
import cluster.management.ServiceRegistry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Application implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 300;
    private static final int DEFAULT_PORT = 8080;
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        int currentServerPort = args.length == 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
        Application app = new Application();
        ZooKeeper zooKeeper = app.connectToZookeeper();

        ServiceRegistry serviceRegistry = new ServiceRegistry(zooKeeper, "/service_registry");
        ServiceRegistry coordinatorServiceRegistry = new ServiceRegistry(zooKeeper, "/coordinator_service_registry");

        OnElectionAction onElectionAction = new OnElectionAction(serviceRegistry, currentServerPort, coordinatorServiceRegistry);

        LeaderElection leaderElection = new LeaderElection(zooKeeper, onElectionAction);
        leaderElection.volunteerForLeadership();
        leaderElection.electLeader();

        app.run();
        app.close();
        System.out.println("Disconnected from Zookeeper, exiting application");
    }

    public ZooKeeper connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        return this.zooKeeper;
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent event) {

    }
}
