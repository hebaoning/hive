package org.apache.hive.hplsql;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hive.hplsql.functions.Function;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class HplServer {

  private static Exec exec;

  static {
    exec = new Exec();
    exec.function = new Function(exec);
    exec.includeRcFile();
  }

  public static HplsqlParser.Create_procedure_stmtContext getProcedure(String name) {
    if (exec == null) {
      return null;
    }
    return exec.function.getProcedure(name);
  }

  public static HplsqlParser.Create_function_stmtContext getFunction(String name) {
    if (exec == null) {
      return null;
    }
    return exec.function.getFunction(name);
  }

  public static int init(String[] args) {
    exec.arguments.parse(args);
    String[] preloadFolders = exec.arguments.getPreloadFolders();
    if (preloadFolders != null) {
      ExecutorService executorService = Executors.newFixedThreadPool(16);
      for (String folder : preloadFolders) {
        System.out.println("preload folder " + folder);
        Iterator<File> files = FileUtils.iterateFiles(
            new File(folder), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE);
        while (files.hasNext()) {
          String file = files.next().getAbsolutePath();
          executorService.execute(new PreloadTask(file));
        }
      }

      try {
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return 0;
  }

  public static void startServer() {
    try {
      System.out.println("hplsql server start...");

      TProcessor tprocessor = new HplService.Processor<HplService.Iface>(new HplServiceImpl());
      TServerSocket serverTransport = new TServerSocket(exec.arguments.getPort());

      //TServer.Args tArgs = new TServer.Args(serverTransport);
      TThreadPoolServer.Args tArgs =
          new TThreadPoolServer.Args(serverTransport).minWorkerThreads(16).maxWorkerThreads(16);
      tArgs.processor(tprocessor);
      tArgs.protocolFactory(new TBinaryProtocol.Factory());

      //TServer server = new TSimpleServer(tArgs);
      TServer server = new TThreadPoolServer(tArgs);
      server.serve();
    } catch (Exception e) {
      System.out.println("hplsql server start error");
      e.printStackTrace();
    }
  }

  static class PreloadTask implements Runnable {

    String file;

    PreloadTask(String file) {
      this.file = file;
    }

    @Override
    public void run() {
      System.out.println("preload " + file);
      exec.includeFile(file, true);
    }
  }
}