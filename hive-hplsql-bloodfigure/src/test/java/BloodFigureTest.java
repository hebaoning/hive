import domain.Relation;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

/*
 *项目名: hive
 *文件名: BloodFigureTest
 *创建者: jianjiejin
 *创建时间:2018/12/18 5:33 PM
 *描述: TODO

 */class BloodFigureTest {
    //临时文件，存放结果
    String tmpFile = "/Users/jianjie/Desktop/jianjiejin/hive/hive-hplsql-bloodfigure/target/tmp/result.txt";

    @Test
    public void testInsert_stmt() throws Exception {
        String testFile = "insert_stmt";
        run(testFile);

    }

    @Test
    public void testInsert_stmt1() throws Exception {
        String testFile = "insert_stmt1";
        run(testFile);

    }

    @Test
    public void testMerge_stmt() throws Exception {
        String testFile = "merge_stmt";
        run(testFile);
    }

    @Test
    public void testCreate_view_stmt() throws Exception {
        String testFile = "create_view_stmt";
        run(testFile);
    }

    /**
     * 运行测试
     * @param testFile
     * @throws Exception
     */
    void run(String testFile) throws Exception {

        InputStream is = System.in;
        is = new FileInputStream("/Users/jianjie/Desktop/jianjiejin/hive/hive-hplsql-bloodfigure/src/test/sql/" + testFile + ".sql");
        ANTLRInputStream input = new ANTLRInputStream(is);
        HplsqlLexer lexer = new HplsqlLexer(input);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        HplsqlParser parser = new HplsqlParser(tokenStream);
        ParseTree tree = parser.program();
        // 自定义visitor遍历
        FigureVisitor visitor = new FigureVisitor();
        visitor.visit(tree);
        Set<Relation> relationSet = visitor.getRelationSet();

        StringBuilder sb = new StringBuilder();
        for (Relation relation : relationSet
        ) {
            sb.append(relation.getFromTable()).append(" ").append(relation.getToTable()).append("\n");
        }
        writeToFile(sb.toString());
        String t = FileUtils.readFileToString(
            new java.io.File("/Users/jianjie/Desktop/jianjiejin/hive/hive-hplsql-bloodfigure/src/test/sqlResult/" + testFile
                + "_result.txt"), "utf-8").trim();

        Assert.assertEquals(t,sb.toString().trim());

    }

    /**
     * 将测试结果写入临时文件
     * @param content
     */
    void writeToFile(String content) {
        BufferedWriter bw = null;
        try {
            FileWriter fs = new FileWriter(tmpFile);
            bw = new BufferedWriter(fs);
            bw.write(content);
            bw.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }




}