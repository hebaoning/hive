import domain.ExportExcelUtil;
import domain.Relation;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Insert_stmtTest {
    public static void main(String[] args) throws IOException {
        //指定目录
        String file =  "hive-hplsql-bloodfigure/src/test/testSql/Insert_stmtTest";
        List<Relation> list = new ArrayList<>();
        ExportExcelUtil<Relation> util = new ExportExcelUtil<Relation>();


            String inputFile=file;
            if(args.length>0) {
                inputFile = args[0];
            }
            InputStream is = System.in;
            if(inputFile !=null) {
                is= new FileInputStream(inputFile);
            }

            ANTLRInputStream input = new ANTLRInputStream(is);
            HplsqlLexer lexer = new HplsqlLexer( input);
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            HplsqlParser parser = new HplsqlParser(tokenStream);
            ParseTree tree = parser.program();
            // 自定义visitor遍历
            TableFigureVisitor visitor = new TableFigureVisitor();
            visitor.visit(tree);
            Set<Relation> relationSet = visitor.getRelationSet();
            for (Relation relation : relationSet
            ) {
                list.add(relation);
            }


        String[] columnNames = { "From", "To"};
        //按指定的Excel版本，文件路径进行输出
        util.exportExcel("存储依赖", columnNames, list, new FileOutputStream("hive-hplsql-bloodfigure/src/test/testResult/Insert_stmt_Result.xls"), ExportExcelUtil.EXCEl_FILE_2007);
}
}
