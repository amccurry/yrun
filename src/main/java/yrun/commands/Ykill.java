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
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import com.google.common.base.Splitter;

public class Ykill {
  static {
    LogUtil.setupLog();
  }

  public static void main(String[] args) throws IOException {
    YarnClient yarnClient = YarnClient.createYarnClient();
    YarnConfiguration yarnConfiguration = new YarnConfiguration();
    yarnClient.init(yarnConfiguration);
    yarnClient.start();
    boolean debug = false;
    try {
      Splitter splitter = Splitter.on('_');
      for (String arg : args) {
        List<String> list = toList(splitter.split(arg));
        if (list.size() != 3) {
          System.err.println("Application Id " + arg + " is not a valid application id.");
        } else {
          String prefix = list.get(0);
          if (!prefix.equals("application")) {
            System.err.println("Application Id " + arg + " is not a valid application id.");
          } else {
            try {
              long clusterTimestamp = Long.parseLong(list.get(1));
              int id = Integer.parseInt(list.get(2));
              ApplicationId applicationId = ApplicationId.newInstance(clusterTimestamp, id);
              yarnClient.killApplication(applicationId);
              System.out.println("Killed\t" + arg + "");
            } catch (Exception e) {
              if (debug) {
                e.printStackTrace();
              }
              System.err.println("Error while trying to kill " + arg + ".");
            }
          }
        }
      }
    } finally {
      yarnClient.stop();
      yarnClient.close();
    }
  }

  private static List<String> toList(Iterable<String> split) {
    List<String> lst = new ArrayList<String>();
    for (String s : split) {
      lst.add(s);
    }
    return lst;
  }
}
