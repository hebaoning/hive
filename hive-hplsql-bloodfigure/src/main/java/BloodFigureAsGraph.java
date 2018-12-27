import domain.GetFileUtil;
import domain.GraphViz;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class BloodFigureAsGraph {

    public static void main(String[] args) throws IOException {

        //指定要读取sql文件所在的目录
        String fileDir =  "/Users/jianjie/Desktop/DW1";

        GetFileUtil getFileUtil = new GetFileUtil();
        List<String> fileList = getFileUtil.getFileList(fileDir);

        GraphViz gv = new GraphViz();
        gv.addln(gv.start_graph());
        for (int i = 0; i < fileList.size(); i++) {
            String inputFile=fileList.get(i);
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
            //自定义visitor遍历
            BloodFigureAsGraphVisitor bloodFigureAsGraphVisitor = new BloodFigureAsGraphVisitor();
            bloodFigureAsGraphVisitor.visit(tree);
            gv.add(bloodFigureAsGraphVisitor.getSour());
        }

        gv.addln(gv.end_graph());
        String type = "pdf";
        gv.decreaseDpi();
        gv.decreaseDpi();
        //指定输出文件名
        String fileName="result";
        //指定输出图片文件路径
        File out = new File("hive-hplsql-bloodfigure/" + fileName + "." + type);
        gv.writeGraphToFile( gv.getGraph( gv.getDotSource(), type ), out );



    }
}
