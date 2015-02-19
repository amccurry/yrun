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
package yrun.commands;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class Yps {

  static {
    LogUtil.setupLog();
  }

  public static void main(String[] args) throws YarnException, IOException {
    YarnClient yarnClient = YarnClient.createYarnClient();
    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    yarnClient.init(yarnConfiguration);
    yarnClient.start();
    try {
      List<ApplicationReport> applications = yarnClient.getApplications();
      for (ApplicationReport applicationReport : applications) {
        ApplicationId applicationId = applicationReport.getApplicationId();
        String user = applicationReport.getUser();
        String queue = applicationReport.getQueue();
        String name = applicationReport.getName();
        YarnApplicationState yarnApplicationState = applicationReport.getYarnApplicationState();
        float progress = applicationReport.getProgress();
        System.out.printf("%s\t%s\t%s\t%s\t%s\t%f%n", toString(applicationId), user, queue, name,
            yarnApplicationState.name(), progress);
      }
    } finally {
      yarnClient.stop();
      yarnClient.close();
    }
  }

  private static String toString(ApplicationId applicationId) {
    return "application_" + applicationId.getClusterTimestamp() + "_" + applicationId.getId();
  }
}
