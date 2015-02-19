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

public class YarnRunnerArgs {
  private int priority = 0;
  private int numberOfContainers = 1;
  private int memory = 128;
  private int vCores = 1;
  private String command;

  public int getPriority() {
    return priority;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }

  public int getNumberOfContainers() {
    return numberOfContainers;
  }

  public void setNumberOfContainers(int numberOfContainers) {
    this.numberOfContainers = numberOfContainers;
  }

  public int getMemory() {
    return memory;
  }

  public void setMemory(int memory) {
    this.memory = memory;
  }

  public int getvCores() {
    return vCores;
  }

  public void setvCores(int vCores) {
    this.vCores = vCores;
  }

  public String getCommand() {
    return command;
  }

  public void setCommand(String command) {
    this.command = command;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((command == null) ? 0 : command.hashCode());
    result = prime * result + memory;
    result = prime * result + numberOfContainers;
    result = prime * result + priority;
    result = prime * result + vCores;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    YarnRunnerArgs other = (YarnRunnerArgs) obj;
    if (command == null) {
      if (other.command != null)
        return false;
    } else if (!command.equals(other.command))
      return false;
    if (memory != other.memory)
      return false;
    if (numberOfContainers != other.numberOfContainers)
      return false;
    if (priority != other.priority)
      return false;
    if (vCores != other.vCores)
      return false;
    return true;
  }

}
