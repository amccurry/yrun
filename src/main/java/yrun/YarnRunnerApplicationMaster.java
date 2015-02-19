/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package yrun;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class YarnRunnerApplicationMaster {

  private static final Log LOG = LogFactory.getLog(YarnRunnerApplicationMaster.class);

  public static void main(String[] args) throws Exception {
    for (int i = 0; i < args.length; i++) {
      System.out.println("arg[" + i + "]=>[" + args[i] + "]");
    }

    YarnRunnerApplicationMaster master = new YarnRunnerApplicationMaster();
    master.run(args);
  }

  // private HttpServer _server;

  public void run(String[] args) throws Exception {
    JsonParser parser = new JsonParser();
    JsonElement element = parser.parse(new FileReader(YarnRunner.MASTER_JSON));

    LOG.info("Json [" + element + "]");

    JsonObject jsonObject = (JsonObject) element;
    int priority = jsonObject.get("priority").getAsInt();
    int numberOfContainers = jsonObject.get("numberOfContainers").getAsInt();
    int memory = jsonObject.get("memory").getAsInt();
    int vCores = jsonObject.get("vCores").getAsInt();
    String command = jsonObject.get("command").getAsString();

    // startHttpServer();
    // InetSocketAddress address = _server.getAddress();
    // LOG.info("Http server started at [" + address + "]");

    // String appHostName = "app-host-name";
    // int appHostPort = address.getPort();
    // String appTrackingUrl = "http://" + address.getHostName() + ":" +
    // appHostPort + "/";
    // LOG.info("App Tracking Url [" + appTrackingUrl + "]");

    // Initialize clients to ResourceManager and NodeManagers
    Configuration conf = new YarnConfiguration();

    AMRMClient<ContainerRequest> rmClient = AMRMClient.createAMRMClient();
    rmClient.init(conf);
    rmClient.start();

    NMClient nmClient = NMClient.createNMClient();
    nmClient.init(conf);
    nmClient.start();

    // Register with ResourceManager
    LOG.info("Register Application Master 0");
    String appHostName = "";
    int appHostPort = 0;
    String appTrackingUrl = "";
    rmClient.registerApplicationMaster(appHostName, appHostPort, appTrackingUrl);
    LOG.info("Register Application Master 1");

    // Priority for worker containers - priorities are intra-application
    Priority priorityRecord = Records.newRecord(Priority.class);
    priorityRecord.setPriority(priority);

    // Resource requirements for worker containers
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(memory);
    capability.setVirtualCores(vCores);

    // Make container requests to ResourceManager
    for (int i = 0; i < numberOfContainers; ++i) {
      ContainerRequest containerAsk = new ContainerRequest(capability, null, null, priorityRecord);
      LOG.info("Making resource request for [" + i + "]");
      rmClient.addContainerRequest(containerAsk);
    }

    // Obtain allocated containers, launch and check for responses
    int responseId = 0;
    int completedContainers = 0;
    long lastReport = 0;

    List<Container> containers = new ArrayList<Container>();

    while (completedContainers < numberOfContainers) {
      if (lastReport + TimeUnit.SECONDS.toNanos(5) < System.nanoTime()) {
        for (Container container : containers) {
          ContainerId containerId = container.getId();
          NodeId nodeId = container.getNodeId();
          ContainerStatus containerStatus = nmClient.getContainerStatus(containerId, nodeId);
          LOG.info("NodeId [" + nodeId + "] Container Status [" + containerStatus + "]");

          // Figure out

        }
        lastReport = System.nanoTime();
      }

      AllocateResponse response = rmClient.allocate(responseId++);
      for (Container container : response.getAllocatedContainers()) {
        containers.add(container);
        // Launch container by create ContainerLaunchContext
        ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
        ctx.setCommands(Collections.singletonList(command + " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR
            + "/stdout2" + " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"));
        LOG.info("Launching container " + container.getId());
        nmClient.startContainer(container, ctx);
      }

      for (ContainerStatus status : response.getCompletedContainersStatuses()) {
        completedContainers++;
        LOG.info("Completed container " + status.getContainerId());
      }
      Thread.sleep(100);
    }
    // _server.stop(0);
    // Un-register with ResourceManager
    rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
  }
  //
  // private void startHttpServer() throws IOException {
  // _server = HttpServer.create(new InetSocketAddress(0), 0);
  // _server.createContext("/", this);
  // _server.setExecutor(null);
  // _server.start();
  // }
  //
  // @Override
  // public void handle(HttpExchange httpExchange) throws IOException {
  // File file = new
  // File("/Users/amccurry/Development/git-project/yarn-runner/src/main/resources/webapp/index.html");
  // httpExchange.sendResponseHeaders(200, file.length());
  // OutputStream os = httpExchange.getResponseBody();
  // FileInputStream input = new FileInputStream(file);
  // IOUtils.copy(input, os);
  // input.close();
  // os.close();
  // }
}
