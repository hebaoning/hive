package org.apache.hive.hplsql;

import org.apache.thrift.TException;

public class HplServiceImpl implements HplService.Iface {

  public HplServiceImpl() {
  }

  @Override
  public String run(String cmd) throws TException {
    Integer code;
    try {
      code = new Exec().run(new String[] {"-trace", "-e", cmd});
      return "DONE: " + code.toString();
    } catch (Exception e) {
      return "ERROR: " + e.getMessage();
    }
  }
}
