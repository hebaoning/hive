import domain.ExportExcelUtil;
import domain.GetFileList;
import domain.Relation;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/*
 *项目名: hive
 *文件名: TableFigureVisitorTest
 *创建者: jianjiejin
 *创建时间:2018/12/17 4:39 PM
 *描述: TODO

 */
class TableFigureVisitorTest {

    @BeforeClass
    public static TableFigureVisitor load() throws IOException {

        /**
         * 测试sql语句:
         * "insert into tb1 select * from b,c;"
         * "insert into t1 select * from b left join c on 1=1 union all d;"
         * "merge into t1 USING (select * from b left join c on 1=1)w
         when matched then
         update set t1.id=1;
         when not matched then
         insert t1 values w.id;"
         * "create view view_name as select id,name from tb1 where 1=1;"
         */
        String sql = "create view view_name as select id,name from tb1 where 1=1;";

        InputStream is = new ByteArrayInputStream(sql.getBytes("UTF-8"));
        TableFigureVisitor visitor = new TableFigureVisitor();
        ANTLRInputStream input = new ANTLRInputStream(is);
        HplsqlLexer lexer = new HplsqlLexer(input);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        HplsqlParser parser = new HplsqlParser(tokenStream);
        ParseTree tree = parser.program();
        // 自定义visitor遍历
        visitor.visit(tree);
        return visitor;
    }


    @Test
    void visitInsert_stmt() {
        try {
            Set<String> tableNameSet = load().tableNameSet;
            if (!tableNameSet.isEmpty()) {
                System.out.println("目标表：");
                for (String tableName : tableNameSet
                ) {
                    System.out.println(tableName);
                }

            } else {
                System.out.println("目标表为空");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    void visitFrom_table_name_clause() {
        try {
            Set<String> fromTableNameSet = load().fromTableNameSet;
            if (!fromTableNameSet.isEmpty()) {
                System.out.println("来源表:");
                for (String fromTableName : fromTableNameSet
                ) {
                    System.out.println(fromTableName);
                }
            } else {
                System.out.println("来源表为空");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    void visitCreate_procedure_stmt() {
        Set<String> procTableNameSet = null;
        try {
            procTableNameSet = load().procNameSet;
            if (!procTableNameSet.isEmpty()) {
                System.out.println("存储过程名:");
                for (String procName : procTableNameSet
                ) {
                    System.out.println(procName);
                }
            } else {
                System.out.println("未获取存储过程名");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Test
    void visitCreate_view_stmt() {
        try {
            Set<String> viewNameSet = load().viewNameSet;
            if (!viewNameSet.isEmpty()) {
                for (String viewName : viewNameSet
                ) {
                    System.out.println(viewName);
                }
            } else {
                System.out.println("无视图表创建");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}