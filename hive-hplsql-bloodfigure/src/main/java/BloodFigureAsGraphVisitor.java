import java.util.HashSet;
import java.util.Set;

public class BloodFigureAsGraphVisitor extends HplsqlBaseVisitor {
    private String tableName = null;
    private String fromTableName = null;
    private String procName = null;

    //用于存放最终结果,最后用于画图输出
    StringBuilder result = new StringBuilder();
    //用于存放影响表,保存结果中使用，会清空
    Set<String> tmpSet = new HashSet<>();
    // 用于结果去重
    Set<String> addedSet = new HashSet<>();

    /**
     * 判断是否是表名，过滤中间表，日志表
     */
    private boolean isTableName(String name) {
        if (!name.startsWith("SESSION.") && !name.equals("ETL_ERRLOG_INFO")
            && !name.equals("ETL.PROCLOG") && !name.startsWith("TMP_")
            &&!name.startsWith("TEMP_")) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 保存表之间的关系，结构定义为 GraphVisitor 能识别的格式
     */
    private void formateSave(String name1, String name2) {
        if (name1.contains("存")) {
            result.append("\"" + name1 + "\"" + " -> ").append("\"" + name2 + "\"").append("[color=red penwidth=2.0]" + "\n");
            addedSet.add(procName + name2);
        } else if (name2.contains("存")){
            result.append("\"" + name1 + "\"" + " -> ").append("\"" + name2 + "\"").append("\n");
            addedSet.add(name1 + procName);
        } else {
            result.append("\"" + name1 + "\"" + " -> ").append("\"" + name2 + "\"").append("\n");
            addedSet.add(name1 + name2);
        }
    }

    /**
     * 保存结果
     */
    private void saveResult(String tableName, Set<String> tmpSet) {
        if (isTableName(tableName)) {
            //判断是否获得存储过程名和影响表
            if (!tmpSet.isEmpty()) {
                if (procName != null) {
                    //先保存存储过程和目标表的关系
                    if (!addedSet.contains(procName + tableName)) {
                        formateSave("存储过程：" + procName, tableName);
                    }
                    //再保存影响表和存储过程的关系
                    for (String fromTableName : tmpSet) {
                        if (!addedSet.contains(fromTableName + procName)) {
                            formateSave(fromTableName, "存储过程：" + procName);
                        }
                    }
                    //保存后，清空 tmpSet
                    tmpSet.clear();
                } else {
                    //不存在存储过程名
                    for (String fromTableName : tmpSet) {
                        if (!addedSet.contains(fromTableName + tableName)) {
                            formateSave(fromTableName, tableName);
                        }
                    }
                    tmpSet.clear();
                }
            }
        }
    }


    /**
     *  每进入一个 insert_stmt,就会调用
     * @param ctx
     * @return
     */
    @Override
    public Object visitInsert_stmt(HplsqlParser.Insert_stmtContext ctx) {
        tableName = ctx.table_name().getText().toUpperCase();
        //返回执行对象，否则会退出遍历，导致结果不完整
        Object ctx2 = visitChildren(ctx);
        return ctx2;
    }

    /**
     * 获取 update 的表名
     * @param ctx
     * @return
     */
    @Override
    public Object visitUpdate_table(HplsqlParser.Update_tableContext ctx) {
        tableName = ctx.table_name().ident().getText();
        Object ctx2 = visitChildren(ctx);
        return ctx2;
    }

    /**
     *  获取影响表,并保存结果
     * @param ctx
     * @return
     */
    @Override
    public Object visitFrom_table_name_clause(HplsqlParser.From_table_name_clauseContext ctx) {
        fromTableName = ctx.table_name().ident().getText().toUpperCase();
        //过滤中间表
        if (!fromTableName.startsWith("SESSION.") && !fromTableName.startsWith("TEMP_")
            && !fromTableName.startsWith("TMP_")) {
            tmpSet.add(fromTableName);
        }
        //保存结果
        if (tableName!=null) {
            saveResult(tableName, tmpSet);
        }

        return visitChildren(ctx);
    }

    /**
     *  获取存储过程名
     * @param ctx
     * @return
     */
    @Override
    public Object visitCreate_procedure_stmt(HplsqlParser.Create_procedure_stmtContext ctx) {
        procName = ctx.ident().get(0).getText().toUpperCase();
        return visitChildren(ctx);
    }

    /**
     *  merge_stmt 分析
     * @param ctx
     * @return
     */
    @Override
    public Object visitMerge_stmt(HplsqlParser.Merge_stmtContext ctx) {
        tableName = ctx.merge_table(0).table_name().ident().getText().toUpperCase();
        Object ctx2 = visitChildren(ctx);
        //本次merge情况，不会调用 visitFrom_table_name_clause 方法
        //需要单独保存结果
        if (ctx.merge_table(1) != null && ctx.merge_table(1).select_stmt() == null) {
            fromTableName = ctx.merge_table(1).table_name().ident().getText().toUpperCase();

            if (!fromTableName.startsWith("SESSION.") && !fromTableName.startsWith("TEMP_")
                && !fromTableName.startsWith("TMP_")) {
                tmpSet.add(fromTableName);
            }

            if (tableName!=null) {
                saveResult(tableName, tmpSet);
            }
        }
        return ctx2;
    }

    /**
     * 获取视图表名
     */
    @Override
    public Object visitCreate_view_stmt(HplsqlParser.Create_view_stmtContext ctx) {
        tableName = ctx.ident().getText().toUpperCase();
        Object ctx2 = visitChildren(ctx);
        return ctx2;
    }

    /**
     * 返回结果
     */
    public String getSour() {
        return result.toString();
    }


}
