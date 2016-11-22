package com.comm.basedSearch.elasticsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ResourceBundle;

/**
 * Created by jasstion on 22/11/2016.
 */
public class EsService {
  final static   ResourceBundle settings = ResourceBundle.getBundle("basedSearch");
  protected final static org.slf4j.Logger mLog = LoggerFactory.getLogger(EsService.class);


  protected TransportClient client=null;

  public EsService() {
    super();


  }
  public EsService(String elasticHosts) {
    super();


  }
  public EsService(String elasticHosts,String userName,String passwd,String clusterName) {
    super();
    String hosts=elasticHosts;

     client = new PreBuiltXPackTransportClient(Settings.builder()
        //.put("cluster.name", "+clusterName+")
        .put("xpack.security.user", ""+userName+":"+passwd+"")
    .build());

    //client=TransportClient.builder().build();

    for(String host:hosts.split(";")){

      try {
        client.addTransportAddress(new InetSocketTransportAddress(
            InetAddress.getByName(host.split(":")[0]), Integer.parseInt(host.split(":")[1])));
      } catch (Exception e) {
        mLog.error("error to get elasticsearch client!");
      }

    }

  }
}
