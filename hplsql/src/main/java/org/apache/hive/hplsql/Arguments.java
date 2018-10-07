/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.hplsql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.ParseException;

public class Arguments {

  private static final int DEFAULT_SERVER_PORT = 8090;
  private static Options options = new Options();

  private CommandLine commandLine;

  String execString;
  String fileName;  
  String main;
  String[] preloadFolders;
  String port;
  Map<String, String> vars = new HashMap<String, String>();

  static {
    // -e 'query'
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("quoted-query-string")
        .withDescription("HPL/SQL from command line")
        .create('e'));

    // -f <file>
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("filename")
        .withDescription("HPL/SQL from a file")
        .create('f'));

    // -preload <sp folder>
    options.addOption(OptionBuilder
        .hasArgs()
        .withArgName("sp-folder")
        .withDescription("Preload sp")
        .create("preload"));

    // -preload <sp folder>
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("port")
        .withDescription("Listening port")
        .create("port"));

    // -main entry_point_name
    options.addOption(OptionBuilder
        .hasArg()
        .withArgName("procname")
        .withDescription("Entry point (procedure or function name)")
        .create("main"));  
    
    // -hiveconf x=y
    options.addOption(OptionBuilder
        .withValueSeparator()
        .hasArgs(2)
        .withArgName("property=value")
        .withLongOpt("hiveconf")
        .withDescription("Value for given property")
        .create());
    
    // Substitution option -d, --define
    options.addOption(OptionBuilder
        .withValueSeparator()
        .hasArgs(2)
        .withArgName("key=value")
        .withLongOpt("define")
        .withDescription("Variable substitution e.g. -d A=B or --define A=B")
        .create('d'));

    // Substitution option --hivevar
    options.addOption(OptionBuilder
        .withValueSeparator()
        .hasArgs(2)
        .withArgName("key=value")
        .withLongOpt("hivevar")
        .withDescription("Variable substitution e.g. --hivevar A=B")
        .create());
    
    // [-version|--version]
    options.addOption(new Option("version", "version", false, "Print HPL/SQL version"));

    // [-trace|--trace]
    options.addOption(new Option("trace", "trace", false, "Print debug information"));
    
    // [-offline|--offline]
    options.addOption(new Option("offline", "offline", false, "Offline mode - skip SQL execution"));

    // [-ast|--ast]
    options.addOption(new Option("ast", "ast", false, "Output AST to ast.jpg"));

    // [-server|--server]
    options.addOption(new Option("server", "server", false, "Run in server mode"));

    // [-H|--help]
    options.addOption(new Option("H", "help", false, "Print help information"));
  }
  
  /**
   * Parse the command line arguments
   */
  public boolean parse(String[] args) {
    try {
      commandLine = new GnuParser().parse(options, args);
      execString = commandLine.getOptionValue('e');
      fileName = commandLine.getOptionValue('f');
      main = commandLine.getOptionValue("main");
      preloadFolders = commandLine.getOptionValues("preload");
      port = commandLine.getOptionValue("port");

      Properties p = commandLine.getOptionProperties("hiveconf");
      for(String key : p.stringPropertyNames()) {
        vars.put(key, p.getProperty(key));
      }
      p = commandLine.getOptionProperties("hivevar");
      for(String key : p.stringPropertyNames()) {
        vars.put(key, p.getProperty(key));
      }
      p = commandLine.getOptionProperties("define");
      for(String key : p.stringPropertyNames()) {
        vars.put(key, p.getProperty(key));
      }
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      return false;
    }
    return true;
  }
  
  /**
   * Get the value of execution option -e
   */
  public String getExecString() {
    return execString;
  }
  
  /**
   * Get the value of file option -f
   */
  public String getFileName() {
    return fileName;
  }
  
  /**
   * Get the value of -main option
   */
  public String getMain() {
    return main;
  }

  public String[] getPreloadFolders() {
    return preloadFolders;
  }

  public int getPort() {
    if (commandLine.hasOption("port")) {
      return Integer.parseInt(port);
    }
    return DEFAULT_SERVER_PORT;
  }
  
  /**
   * Get the variables
   */
  public Map<String, String> getVars() {
    return vars;
  }

  /**
   * Test whether version option is set
   */
  public boolean hasVersionOption() {
    return commandLine.hasOption("version");
  }

  /**
   * Test whether ast option is set
   */
  public boolean hasAstOption() {
    return commandLine.hasOption("ast");
  }

  public boolean hasServerOption() {
    return commandLine.hasOption("server");
  }

  /**
   * Test whether debug option is set
   */
  public boolean hasTraceOption() {
    if(commandLine.hasOption("trace")) {
      return true;
    }
    return false;
  }
  
  /**
   * Test whether offline option is set
   */
  public boolean hasOfflineOption() {
    if(commandLine.hasOption("offline")) {
      return true;
    }
    return false;
  }
  
  /**
   * Test whether help option is set
   */
  public boolean hasHelpOption() {
    if(commandLine.hasOption('H')) {
      return true;
    }
    return false;
  }
  
  /**
   * Print help information
   */
  public void printHelp() {
    new HelpFormatter().printHelp("hplsql", options);
  }
}
