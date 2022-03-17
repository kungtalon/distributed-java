package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ServiceRegistry implements Watcher {
    public static final String COORDINATOR_REGISTRY_ZNODE = "/coordinator_service_registry";
    private final ZooKeeper zooKeeper;
    private String currentZnode = null;
    private String serviceRegistryZnode;
    private Random random;

    // cache
    private List<String> allServiceAddress = null;

    public ServiceRegistry(ZooKeeper zooKeeper, String serviceRegistryZnode) {
        this.zooKeeper = zooKeeper;
        this.serviceRegistryZnode = serviceRegistryZnode;
        this.random = new Random();
        createServiceRegistryZnode();
    }

    public void registerToCluster(String metadata) throws InterruptedException, KeeperException {
        this.currentZnode = zooKeeper.create(serviceRegistryZnode + "/n", metadata.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Registered to service registry");
    }

    public void registerForUpdates() {
        try {
            updateAddress();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    public synchronized String getRandomServiceAddress() throws KeeperException, InterruptedException {
        if (allServiceAddress == null) {
            updateAddress();
        }
        if (!allServiceAddress.isEmpty()) {
            int randomIndex = random.nextInt(allServiceAddress.size());
            return allServiceAddress.get(randomIndex);
        } else {
            return null;
        }
    }

    public synchronized List<String> getAllServiceAddress() throws InterruptedException, KeeperException {
        if (allServiceAddress == null) {
            updateAddress();
        }
        return allServiceAddress;
    }

    public void unregisterFromCluster() {
        try{
            if (currentZnode != null && zooKeeper.exists(currentZnode, false) != null) {
                zooKeeper.delete(currentZnode, -1);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    private void createServiceRegistryZnode() {
        try {
            if (zooKeeper.exists(serviceRegistryZnode, false) == null) {
                zooKeeper.create(serviceRegistryZnode, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    private synchronized void updateAddress() throws KeeperException, InterruptedException {
        List<String> workerZnodes = zooKeeper.getChildren(serviceRegistryZnode, this);
        List<String> addresses = new ArrayList<>(workerZnodes.size());

        for (String workerZnode : workerZnodes) {
            String workerZnodeFullPath = serviceRegistryZnode + "/" + workerZnode;
            Stat stat = zooKeeper.exists(workerZnodeFullPath, false);
            if (stat == null) {
                continue;
            }
            byte[] addressBytes = zooKeeper.getData(workerZnodeFullPath, false, stat);
            addresses.add(new String(addressBytes));
        }

        this.allServiceAddress = Collections.unmodifiableList(addresses);
        System.out.println("The cluster addresses are : " + this.allServiceAddress);
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            updateAddress();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
