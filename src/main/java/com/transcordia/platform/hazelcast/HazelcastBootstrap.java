package com.transcordia.platform.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.transcordia.platform.hazelcast.persistence.DelegatingMapStoreFactory;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HazelcastBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(HazelcastBootstrap.class);

    private Client _esClient;
    private HazelcastInstance _hazelcast;
    private DelegatingMapStoreFactory _mapStoreFactory;

    // Constructors ----------------------------------------------------------------

    public HazelcastBootstrap() {
    }

    public HazelcastBootstrap(Client esClient) {
        _esClient = esClient;
    }

    // Properties ------------------------------------------------------------------

    public void setMapStoreFactory(DelegatingMapStoreFactory mapStoreFactory) {
        _mapStoreFactory = mapStoreFactory;
    }

    public DelegatingMapStoreFactory getMapStoreFactory() {
        return _mapStoreFactory;
    }

    // Publics ---------------------------------------------------------------------

    public void init() throws Exception {
        if (LOG.isInfoEnabled()) {
            LOG.info("----------------------------------------");
            LOG.info("Starting Hazelcast bootstrap process");
        }

        final Config config = generateConfig();
        addMapStoreFactory(config, _mapStoreFactory);
        _hazelcast = Hazelcast.init(config);


        if (LOG.isInfoEnabled()) {
            final StringBuilder sb = new StringBuilder();
            final Set<Member> members = _hazelcast.getCluster().getMembers();
            for (Member member : members) {
                final InetAddress address = member.getInetAddress();
                sb.append(address.toString()).append(", ");
            }
            if (members.size() > 0) {
                sb.setLength(sb.length() - 2);
            } else {
                sb.append("No members identified.");
            }

            LOG.info(String.format(
                    "Hazelcast node [%s] joined the cluster. Members: %s",
                    _hazelcast.getName(), sb.toString()
                    ));
            LOG.info("Hazelcast bootstrap process completed");
            LOG.info("----------------------------------------");
        }
    }

    public void destroy() {
        LOG.info("Stopping Hazelcast services");
        Hazelcast.shutdownAll();
        LOG.info("Hazelcast services stopped");
    }

    public HazelcastInstance getHazelcast() {
        return _hazelcast;
    }

    // Protecteds ------------------------------------------------------------------

    protected void addMapStoreFactory(Config config, DelegatingMapStoreFactory factory) {
        if (factory == null) return;

        Map<String, MapConfig> mapConfigs = config.getMapConfigs();
        for (Map.Entry<String, MapConfig> entry : mapConfigs.entrySet()) {
            if (entry.getValue().getMapStoreConfig() != null)
                entry.getValue().getMapStoreConfig().setFactoryImplementation(factory);
        }
    }

    protected Config generateConfig() {

        // Reads in the settings from hazelcast.xml found on the classpath
        Config config = new XmlConfigBuilder().build();

        // Get the tcpip section from the config
        final TcpIpConfig ipConfig = config.getNetworkConfig().getJoin().getTcpIpConfig();

        // Check if config is set to use IP-based discovery and we have an Elastic Search client
        if (ipConfig.isEnabled() && _esClient != null) {
            // Get the IP addresses of other nodes in the cluster as determined by Elastic Search
            final List<String> ips = getClusterIPAddresses();

            // Inject the IP addresses
            config.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(ips);
        }

        return config;
    }

    protected List<String> getClusterIPAddresses() {
        // Perform a cluster health check to get the nodes in the cluster
        ClusterStateResponse response = _esClient.admin().cluster().prepareState()
                .setFilterMetaData(true)
                .setFilterRoutingTable(true)
                .execute().actionGet();

        final ArrayList<String> result = new ArrayList<String>();

        // This is the list of cluster nodes
        final DiscoveryNodes nodes = response.state().getNodes();
        for (DiscoveryNode node : nodes) {
            // There should be one data node per deployed web application
            if (node.isDataNode()) {
                final TransportAddress transport = node.getAddress();
                if (transport instanceof InetSocketTransportAddress) {
                    final InetSocketTransportAddress address = (InetSocketTransportAddress) transport;
                    // This is pretty funny looking. Unfortunate lack of imagination on everyone's part.
                    final String ip = address.address().getAddress().getHostAddress();
                    result.add(ip);
                }
            }
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("The following list of IP addresses were identified in the cluster: " + result.toString());
        }

        return result;
    }

}
