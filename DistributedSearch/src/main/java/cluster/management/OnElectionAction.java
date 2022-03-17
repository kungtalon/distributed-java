package cluster.management;

import networking.OnRequestHandler;
import networking.WebClient;
import networking.WebServer;
import org.apache.zookeeper.KeeperException;
import search.SearchCoordinator;
import search.SearchWorkerHandler;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class OnElectionAction implements OnElectionCallback {
    private final ServiceRegistry serviceRegistry;
    private final ServiceRegistry coordinatorServiceRegistry;
    private final int port;
    private WebServer webServer;

    public OnElectionAction(ServiceRegistry serviceRegistry, int port, ServiceRegistry coordinatorServiceRegistry) {
        this.serviceRegistry = serviceRegistry;
        this.port = port;
        this.coordinatorServiceRegistry = coordinatorServiceRegistry;
    }

    @Override
    public void onElectedToBeLeader() {
        serviceRegistry.unregisterFromCluster();
        serviceRegistry.registerForUpdates();

        if (webServer != null) {
            webServer.stop();
        }

        SearchCoordinator searchCoordinator = new SearchCoordinator(serviceRegistry, new WebClient());
        webServer = new WebServer(port, searchCoordinator);
        webServer.startServer();

        try {
            String currentServerAddress = String.format(
                    "http://%s:%d%s",
                    InetAddress.getLocalHost().getCanonicalHostName(),
                    port,
                    searchCoordinator.getEndpoint());
            coordinatorServiceRegistry.registerToCluster(currentServerAddress);
        } catch (UnknownHostException | KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onWorker() {
        if (webServer != null) {
            return;
        }
        OnRequestHandler workerHandler = new SearchWorkerHandler();
        webServer = new WebServer(port, workerHandler);
        webServer.startServer();

        try {
            String currentServerAddress = String.format("http://%s:%d%s",
                    InetAddress.getLocalHost().getCanonicalHostName(), port, workerHandler.getEndpoint());
            serviceRegistry.registerToCluster(currentServerAddress);
        } catch (UnknownHostException | InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}
