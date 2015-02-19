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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Splitter;
import com.google.gson.Gson;

public class YarnRunnerUtil {

  public static final String TMP_YARNRUNNER_JOB = "tmp-yarnrunner-job_";
  public static final String JAR = ".jar";
  public static final String SEP = "/";
  public static final String PATH_SEPARATOR = "path.separator";
  public static final String JAVA_CLASS_PATH = "java.class.path";

  public static Path getJarLocalPathOrCreate(Class<? extends YarnRunner> c) throws IOException {
    ClassLoader classLoader = YarnRunnerUtil.class.getClassLoader();
    String resourceName = toResourceName(c);
    URL url = classLoader.getResource(resourceName);
    if (isJarFile(url)) {
      return getJarPath(url);
    } else {
      return buildJar(url, resourceName);
    }
  }

  private static Path buildJar(URL url, String resourceName) throws IOException {
    String classPath = System.getProperty(JAVA_CLASS_PATH);
    String pathSeparator = System.getProperty(PATH_SEPARATOR);
    Splitter splitter = Splitter.on(pathSeparator);
    for (String path : splitter.split(classPath)) {
      File file = new File(new File(path), resourceName);
      if (file.exists()) {
        return buildJarFromClassFile(file, resourceName);
      }
    }
    throw new IOException("Resource [" + resourceName + "] not found in classpath.");
  }

  private static Path buildJarFromClassFile(File classFile, String resourceName) throws IOException {
    String absolutePath = classFile.getAbsolutePath();
    if (absolutePath.endsWith(resourceName)) {
      File file = new File(absolutePath.substring(0, absolutePath.length() - resourceName.length()));
      if (file.exists() && file.isDirectory()) {
        return constructJar(file);
      }
    }
    throw new RuntimeException("unkown error");
  }

  private static Path constructJar(File sourceFile) throws IOException {
    if (sourceFile.isDirectory()) {
      File file = File.createTempFile(TMP_YARNRUNNER_JOB, JAR);
      OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));
      JarOutputStream jarOut = new JarOutputStream(outputStream);
      for (File f : sourceFile.listFiles()) {
        pack(sourceFile, f, jarOut);
      }
      jarOut.close();
      file.deleteOnExit();
      return new Path(file.toURI());
    }
    throw new RuntimeException("File [" + sourceFile + "] is not a directory.");
  }

  private static void pack(File rootPath, File source, JarOutputStream target) throws IOException {
    String name = getName(rootPath, source);
    if (source.isDirectory()) {
      if (!SEP.equals(name)) {
        JarEntry entry = new JarEntry(name);
        entry.setTime(source.lastModified());
        target.putNextEntry(entry);
        target.closeEntry();
      }
      for (File f : source.listFiles()) {
        pack(rootPath, f, target);
      }
    } else {
      JarEntry entry = new JarEntry(name);
      entry.setTime(source.lastModified());
      target.putNextEntry(entry);
      BufferedInputStream in = new BufferedInputStream(new FileInputStream(source));
      IOUtils.copy(in, target);
      in.close();
      target.closeEntry();
    }
  }

  private static String getName(File rootPath, File source) {
    String rootStr = rootPath.toURI().toString();
    String sourceStr = source.toURI().toString();
    if (sourceStr.startsWith(rootStr)) {
      String result = sourceStr.substring(rootStr.length());
      if (source.isDirectory() && !result.endsWith(SEP)) {
        result += SEP;
      }
      return result;
    } else {
      throw new RuntimeException("Not sure what happened.");
    }
  }

  private static Path getJarPath(URL url) throws IOException {
    URI uri;
    try {
      uri = url.toURI();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    String schemeSpecificPart = uri.getSchemeSpecificPart();
    String filePath = schemeSpecificPart.substring(0, schemeSpecificPart.indexOf('!'));
    return new Path(filePath);
  }

  private static boolean isJarFile(URL url) throws IOException {
    URI uri;
    try {
      uri = url.toURI();
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
    String scheme = uri.getScheme();
    if (scheme.equals("jar")) {
      return true;
    }
    return false;
  }

  private static String toResourceName(Class<? extends YarnRunner> c) {
    return c.getName().replace('.', '/') + ".class";
  }

  public static Path writeAndGetPath(YarnRunnerArgs yarnRunnerArgs) throws IOException {
    Gson gson = new Gson();
    String json = gson.toJson(yarnRunnerArgs);
    File file = File.createTempFile(TMP_YARNRUNNER_JOB, ".json");
    FileWriter fileWriter = new FileWriter(file);
    fileWriter.write(json);
    fileWriter.close();
    file.deleteOnExit();
    return new Path(file.toURI());
  }

}
