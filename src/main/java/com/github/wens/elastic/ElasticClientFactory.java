package com.github.wens.elastic;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;


/**
 * Created by wens on 15-10-15.
 */
public class ElasticClientFactory {

    public static ElasticClient create(String clusterName, String[] serverAddresses) {

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.sniff", true).put("cluster.name", clusterName).build();

        TransportClient transportClient = new TransportClient(settings);

        for (int i = 0; i < serverAddresses.length; i++) {
            String[] hostPort = serverAddresses[i].trim().split(":");
            String host = hostPort[0].trim();
            int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim())
                    : 9300;
            transportClient.addTransportAddress(new InetSocketTransportAddress(host, port));
        }

        transportClient.connectedNodes();


        return new ElasticClient(transportClient);

    }


}
