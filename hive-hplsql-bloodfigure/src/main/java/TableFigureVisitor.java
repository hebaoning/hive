import domain.Relation;

import java.util.HashSet;
import java.util.Set;


public class TableFigureVisitor extends HplsqlBaseVisitor {

    private String tableName = null;
    private String procName = null;
    private Set<Relation> relationsSet = new HashSet<Relation>();   //用于存放结果
    private Set<String> set = new HashSet<>();              //用于存放影响表
    private Set<String> value = new HashSet<>();            // 用于结果去重

    /**
     * 将值传入 relationsSet 集合，作为最后结果的返回
     *
     * @param name1
     * @param name2
     */
    private void insertRelationsSet(String name1, String name2) {
        Relation relation = new Relation();
        relation.setFromTable(name1);
        relation.setToTable(name2);
        relationsSet.add(relation);
        value.add(name1 + name2);
    }

    /**
     * 存放结果, 在 visitInsert_stmt， visitMerge_stmt 方法中会被调用
     */
    private void saveResult() {
        //存影响表的 set 非空时候，遍历 set 保存影响表名和存储过程名
        if (!set.isEmpty()) {
        for (String str : set) {
            if (!str.contains("SESSION.") && !value.contains(str + procName)) {
                insertRelationsSet(str, procName);
            }
        }
        }
        //目标表非null时候，保存存储过程名和目标表名
        if (tableName != null && !tableName.contains("SESSION.") && !tableName.contains("ETL_ERRLOG_INFO")
                && !value.contains(procName + tableName) && !tableName.contains("ETL.PROCLOG")) {
            insertRelationsSet(procName, tableName);
            //set 清空
            set.clear();
        }
    }

//    /*
//        处理过程函数,格式定义为dot语言
//     */
//
//    private void main(){
//        for (String str : set) {
//            sb.append("\"" + str + "\"");
//            if (!set.isEmpty()  && !str.toUpperCase().contains("SESSION.")) {
//                sb.append(" -> " + "\"" + "存储过程：" + procName + "\"" + "\n");
//                if (!sourLineSet.contains(sb.toString())) {
//                    source.append(sb.toString());
//                    sourLineSet.add(sb.toString());
//                }
//            }
//            sb.delete(0, sb.length());
//        }
//
//        if(tableName!=null && !tableName.toUpperCase().contains("SESSION.") && !tableName.contains("ETL_ERRLOG_INFO")) {
//            sb.append("\"" + "存储过程：" + procName + "\"" + " -> " + "\"" + tableName + "\"" + "[color=red penwidth=2.0]" + "\n");
//            if (!sourLineSet.contains(sb.toString())) {
//                source.append(sb);
//                sourLineSet.add(sb.toString());
//            }
//            sb.delete(0, sb.length());
//            set.clear();
//        }
//
//    }


    /**
     * 进入insert_stmt，获取目标表
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitInsert_stmt(HplsqlParser.Insert_stmtContext ctx) {
        tableName = ctx.table_name().getText().toUpperCase();
        //先保存一次结果
        saveResult();
        //返回执行对象，否则会退出遍历
        Object ctx2= visitChildren(ctx);
        //遍历之后再保存一次结果
        saveResult();
        return ctx2;
    }

    /**
     * 获取影响表
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitFrom_table_name_clause(HplsqlParser.From_table_name_clauseContext ctx) {
        String fromTableName = ctx.table_name().ident().getText().toUpperCase();
        set.add(fromTableName);
        return visitChildren(ctx);
    }

    /**
     * 获取存储过程名
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitCreate_procedure_stmt(HplsqlParser.Create_procedure_stmtContext ctx) {
        procName = ctx.ident().get(0).getText().toUpperCase();
        return visitChildren(ctx);
    }

    /**
     * 获取 mergeTableName
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitMerge_stmt(HplsqlParser.Merge_stmtContext ctx) {
        tableName = ctx.merge_table(0).table_name().ident().getText().toUpperCase();
        saveResult();
        Object ctx2= visitChildren(ctx);
        saveResult();
        return ctx2;
    }

    /**
     * 获取视图表名,不需要调用 save() 函数，直接存入结果
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitCreate_view_stmt(HplsqlParser.Create_view_stmtContext ctx) {
        String viewName = ctx.ident().getText().toUpperCase();
        Object ctx2=visitChildren(ctx);
        for (String tableName : set) {
            insertRelationsSet(tableName, viewName);
        }
        set.clear();
        return ctx2;
    }

    /**
     * 返回结果集合的方法
     *
     * @return
     */
    public Set<Relation> getRelationSet() {
        return relationsSet;
    }

}
