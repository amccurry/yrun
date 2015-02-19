package yrun;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import yrun.commands.LogUtil;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

public class YarnRunner {

  static {
    LogUtil.setupLog();
  }

  static final String MASTER_JSON = "master.json";
  private static final String YARN_RESOURCEMANAGER_ADDRESS = "yarn.resourcemanager.address";
  private static final Log LOG = LogFactory.getLog(YarnRunner.class);

  public static void main(final String[] args) throws IOException, InterruptedException {
    CommandLine cmd = parse(args, new PrintWriter(System.out));
    final String yarnName = cmd.getOptionValue("n");
    LOG.info("Yarn name [" + yarnName + "]");
    String resourceManagerAddress = cmd.getOptionValue("rma");
    LOG.info("Resource Manager Address [" + resourceManagerAddress + "]");
    String installPathStr = cmd.getOptionValue("p");
    LOG.info("Install Path [" + installPathStr + "]");
    final String command = StringUtils.join(" ", cmd.getOptionValues("c"));
    LOG.info("Command [" + command + "]");
    final String queue;
    if (cmd.hasOption("q")) {
      queue = cmd.getOptionValue("q");
    } else {
      queue = "default";
    }
    LOG.info("Queue [" + queue + "]");
    final YarnConfiguration configuration = new YarnConfiguration();
    configuration.set(YARN_RESOURCEMANAGER_ADDRESS, resourceManagerAddress);
    LOG.info("Using resource manager [" + resourceManagerAddress + "]");

    final Path installPath = new Path(installPathStr);

    final List<Path> archivePathList = new ArrayList<Path>();
    if (cmd.hasOption("a")) {
      String[] archivePaths = cmd.getOptionValues("a");
      for (String archivePath : archivePaths) {
        archivePathList.add(new Path(archivePath));
      }
    }

    final boolean isDaemon = !cmd.hasOption("k");

    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    ugi.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        YarnRunner yarnRunner = new YarnRunner(configuration, installPath, archivePathList, command, yarnName, queue,
            isDaemon);
        yarnRunner.execute();
        return null;
      }
    });
  }

  @SuppressWarnings("static-access")
  public static CommandLine parse(String[] otherArgs, Writer out) {
    Options options = new Options();
    options.addOption(OptionBuilder.isRequired().withArgName("name").hasArg()
        .withDescription("The name of yarn application.").create("n"));
    options.addOption(OptionBuilder.isRequired().withArgName("resourceManagerAddress").hasArg()
        .withDescription("The address of the yarn resource manager.").create("rma"));
    options.addOption(OptionBuilder.isRequired().withArgName("path").hasArg()
        .withDescription("The path where this application will be installed in yarn.").create("p"));
    options.addOption(OptionBuilder.withArgName("queue").hasArg()
        .withDescription("The yarn queue to execute this application.").create("q"));
    options.addOption(OptionBuilder.isRequired().withArgName("command").hasArgs()
        .withDescription("The command to execute in the yarn application.").create("c"));
    options.addOption(OptionBuilder.withArgName("archive").hasArgs()
        .withDescription("The archive(s) to delivery and extract in the application.").create("a"));
    options.addOption(OptionBuilder.withArgName("kill").withDescription("Kill the application on client death.")
        .create("k"));
    options.addOption(OptionBuilder.withArgName("help").withDescription("Displays help for this command.").create("h"));

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, otherArgs);
      if (cmd.hasOption("h")) {
        HelpFormatter formatter = new HelpFormatter();
        PrintWriter pw = new PrintWriter(out, true);
        formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, "run", null, options, HelpFormatter.DEFAULT_LEFT_PAD,
            HelpFormatter.DEFAULT_DESC_PAD, null, false);
        return null;
      }
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      PrintWriter pw = new PrintWriter(out, true);
      formatter.printHelp(pw, HelpFormatter.DEFAULT_WIDTH, "run", null, options, HelpFormatter.DEFAULT_LEFT_PAD,
          HelpFormatter.DEFAULT_DESC_PAD, null, false);
      return null;
    }
    return cmd;
  }

  private final YarnConfiguration _configuration;
  private String _appJarFile = "yrun.jar";
  private final Path _installPath;
  private final List<Path> _archivePathList;
  private final String _command;
  private final String _yarnName;
  private final String _queue;
  private final boolean _isDaemon;
  private final FileSystem _fileSystem;

  public YarnRunner(YarnConfiguration configuration, Path installPath, List<Path> archivePathList, String command,
      String yarnName, String queue, boolean isDaemon) throws IOException {
    _configuration = configuration;
    _fileSystem = installPath.getFileSystem(_configuration);
    _installPath = makeQualified(installPath, _fileSystem);
    _archivePathList = archivePathList;
    _command = command;
    _yarnName = yarnName;
    _queue = queue;
    _isDaemon = isDaemon;
  }

  public void execute() throws IOException, YarnException, InterruptedException {
    LOG.info("Using application path [" + _installPath + "]");
    Path jarPath = installThisJar(_installPath, _appJarFile);
    LOG.info("Driver installed [" + jarPath + "]");
    List<Path> installedArchivePathList = install(_installPath, _archivePathList);
    for (Path p : installedArchivePathList) {
      LOG.info("Archive installed [" + p + "]");
    }

    YarnRunnerArgs yarnRunnerArgs = new YarnRunnerArgs();
    yarnRunnerArgs.setCommand(_command);

    Path argsPath = installThisArgs(_installPath, yarnRunnerArgs);

    final YarnClient client = YarnClient.createYarnClient();
    _configuration.setInt("yarn.nodemanager.delete.debug-delay-sec", (int) TimeUnit.HOURS.toSeconds(1));
    client.init(_configuration);
    client.start();

    YarnClientApplication app = client.createApplication();
    ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

    Map<String, String> appMasterEnv = new HashMap<String, String>();
    setupAppMasterEnv(appMasterEnv, _appJarFile);

    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    {
      LocalResource appMasterJar = Records.newRecord(LocalResource.class);
      setupAppMasterJar(jarPath, appMasterJar);
      localResources.put(jarPath.getName(), appMasterJar);
    }
    {
      LocalResource appMasterArgs = Records.newRecord(LocalResource.class);
      setupAppMasterArgs(argsPath, appMasterArgs);
      localResources.put(MASTER_JSON, appMasterArgs);
    }

    List<String> vargs = new ArrayList<String>();
    vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
    vargs.add("-Xmx256m");
    vargs.add("-Djava.net.preferIPv4Stack=true");
    vargs.add(YarnRunnerApplicationMaster.class.getName());

    String strCommand = "(echo ENV && set && echo CURRENT_DIR_LISTING && ls -la && echo PWD && pwd && ("
        + StringUtils.join(" ", vargs) + "))";
    strCommand += " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout";
    strCommand += " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
    LOG.debug("Application Master command [" + strCommand + "]");

    amContainer.setCommands(Collections.singletonList(strCommand));
    amContainer.setLocalResources(localResources);
    amContainer.setEnvironment(appMasterEnv);

    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(256);
    capability.setVirtualCores(1);

    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    appContext.setApplicationName(_yarnName);
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(capability);
    if (_queue != null) {
      appContext.setQueue(_queue);
    }
    appContext.setApplicationType("yrun");

    ApplicationId appId = appContext.getApplicationId();
    AtomicBoolean shutdown = new AtomicBoolean();
    if (!_isDaemon) {
      addShutdownHook(client, appId, shutdown);
    }

    LOG.info("Submitting application with id [" + appId + "]");
    client.submitApplication(appContext);
    ApplicationReport report;
    YarnApplicationState state;
    do {
      report = client.getApplicationReport(appId);
      state = report.getYarnApplicationState();
      if (state == YarnApplicationState.RUNNING) {
        if (_isDaemon) {
          LOG.info("Application is running.  This is a daemon application driver program exiting.");
          return;
        }
      }
      Thread.sleep(100);
    } while (isNoLongerRunning(state));
    shutdown.set(true);
    LOG.info("Application has finished with state [" + state + "]");
  }

  private boolean isNoLongerRunning(YarnApplicationState state) {
    return state != YarnApplicationState.FINISHED && state != YarnApplicationState.KILLED
        && state != YarnApplicationState.FAILED;
  }

  private void addShutdownHook(final YarnClient client, final ApplicationId appId, final AtomicBoolean shutdown) {
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        if (shutdown.get()) {
          return;
        }
        LOG.info("Application is shutting down, killing app [" + appId + "]");
        try {
          client.killApplication(appId);
        } catch (Exception e) {
          LOG.error("Unknown error while trying to kill application during shutdown.", e);
        }
      }
    }));
  }

  private Path installThisArgs(Path installPath, YarnRunnerArgs yarnRunnerArgs) throws IOException {
    Path localPath = YarnRunnerUtil.writeAndGetPath(yarnRunnerArgs);
    return copy(localPath, new Path(installPath, MASTER_JSON));
  }

  private void setupAppMasterArgs(Path yarnRunnerArgsPath, LocalResource appMasterArgs) throws IOException {
    FileSystem fileSystem = yarnRunnerArgsPath.getFileSystem(_configuration);
    FileStatus jarStatus = fileSystem.getFileStatus(yarnRunnerArgsPath);
    appMasterArgs.setResource(ConverterUtils.getYarnUrlFromPath(yarnRunnerArgsPath));
    appMasterArgs.setSize(jarStatus.getLen());
    appMasterArgs.setTimestamp(jarStatus.getModificationTime());
    appMasterArgs.setType(LocalResourceType.FILE);
    appMasterArgs.setVisibility(LocalResourceVisibility.PUBLIC);
  }

  private void setupAppMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
    FileSystem fileSystem = jarPath.getFileSystem(_configuration);
    FileStatus jarStatus = fileSystem.getFileStatus(jarPath);
    appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
    appMasterJar.setSize(jarStatus.getLen());
    appMasterJar.setTimestamp(jarStatus.getModificationTime());
    appMasterJar.setType(LocalResourceType.FILE);
    appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
  }

  private void setupAppMasterEnv(Map<String, String> appMasterEnv, String appJarFile) {
    String dirSep = File.separator;
    String pathSep = File.pathSeparator;
    for (String c : _configuration.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), c.trim(), pathSep);
    }
    Apps.addToEnvironment(appMasterEnv, Environment.JAVA_HOME.name(),
        "/Library/Java/JavaVirtualMachines/jdk1.7.0_40.jdk/Contents/Home", pathSep);
    Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), Environment.PWD.$() + dirSep + appJarFile,
        pathSep);
    Apps.addToEnvironment(appMasterEnv, Environment.CLASSPATH.name(), Environment.PWD.$() + dirSep + "*", pathSep);
  }

  private List<Path> install(Path installPath, List<Path> archivePathList) throws IOException {
    List<Path> result = new ArrayList<Path>();
    for (Path p : archivePathList) {
      result.add(copy(p, new Path(installPath, p.getName())));
    }
    return result;
  }

  private Path installThisJar(Path installPath, String runnerJarName) throws IOException {
    Class<? extends YarnRunner> c = getClass();
    Path jarLocalPath = YarnRunnerUtil.getJarLocalPathOrCreate(c);
    return copy(jarLocalPath, new Path(installPath, runnerJarName));
  }

  private Path copy(Path src, Path dest) throws IOException {
    FileSystem srcFileSystem = src.getFileSystem(_configuration);
    FileSystem destFileSystem = dest.getFileSystem(_configuration);
    src = makeQualified(src, srcFileSystem);
    dest = makeQualified(dest, destFileSystem);
    FSDataOutputStream outputStream = destFileSystem.create(dest);
    FSDataInputStream inputStream = srcFileSystem.open(src);
    IOUtils.copy(inputStream, outputStream);
    inputStream.close();
    outputStream.close();
    return dest;
  }

  private Path makeQualified(Path p, FileSystem fileSystem) {
    return p.makeQualified(fileSystem.getUri(), fileSystem.getWorkingDirectory());
  }

}
