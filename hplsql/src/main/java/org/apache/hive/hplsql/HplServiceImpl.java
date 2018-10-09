package org.apache.hive.hplsql;

import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;

public class HplServiceImpl implements HplService.Iface {

  public HplServiceImpl() {
  }

  @Override
  public String run(String cmd) throws TException {
    Integer code;
    try {
      List<String> argList = new ArrayList<>(Hplsql.argList);
      argList.add("-e");
      argList.add(cmd);
      code = new Exec().run(argList.toArray(new String[0]));
      return "DONE: " + code.toString();
    } catch (Exception e) {
      return "ERROR: " + e.getMessage();
    }
  }
}
