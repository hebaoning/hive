import domain.*;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class BloodFigure {

    public static void main(String[] args) throws IOException {
        //指定目录
        String fileDir = "/Users/jianjie/Desktop/test类/";

        List<String> fileList = GetFileUtil.getFileList(fileDir);
        List<Relation> list = new ArrayList<>();
        ExportExcelUtil<Relation> util = new ExportExcelUtil<Relation>();

        for (int i = 0; i < fileList.size(); i++) {

            String inputFile = fileList.get(i);
            if (args.length > 0) {
                inputFile = args[0];
            }
            InputStream is = System.in;
            if (inputFile != null) {
                is = new FileInputStream(inputFile);
            }

            ANTLRInputStream input = new ANTLRInputStream(is);
            HplsqlLexer lexer = new HplsqlLexer(input);
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            HplsqlParser parser = new HplsqlParser(tokenStream);
            ParseTree tree = parser.program();
            // 自定义visitor遍历
            FigureVisitor visitor = new FigureVisitor();
            visitor.visit(tree);
            Set<Relation> relationSet = visitor.getRelationSet();
            for (Relation relation : relationSet
            ) {
                list.add(relation);
            }
        }

        String[] columnNames = {"From", "To"};
        //按指定的Excel版本，文件路径进行输出
        util.exportExcel("存储依赖", columnNames, list, new FileOutputStream("/Users/jianjie/Desktop/test/test3.xls"), ExportExcelUtil.EXCEl_FILE_2007);

    }
}

