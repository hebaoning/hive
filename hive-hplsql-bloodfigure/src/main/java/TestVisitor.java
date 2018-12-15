import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import sun.jvm.hotspot.debugger.cdbg.ObjectVisitor;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by huangchaoyuan on 2018/12/12.
 */
public class TestVisitor extends HplsqlBaseVisitor {
    private String toTable = "";
    private String fromTable = "";
    private boolean firstMergeTableHasTableName = true;
    private boolean nextMergeTableHasTableName = true;
    private boolean finishFirst = false;

    @Override
    public Object visitMerge_stmt(HplsqlParser.Merge_stmtContext ctx) {
        if (ctx.merge_table(0).table_name() != null) {
            toTable = ctx.merge_table(0).table_name().ident().getText();
        } else {
            firstMergeTableHasTableName = false;
        }

        if (ctx.merge_table(1).table_name() != null) {
            fromTable = ctx.merge_table(1).table_name().ident().getText();
        } else {
            nextMergeTableHasTableName = false;
        }
        return visitChildren(ctx);
    }

    @Override
    public Object visitFrom_table_name_clause(HplsqlParser.From_table_name_clauseContext ctx) {
        System.out.println(ctx.table_name().ident().getText());

        if (!nextMergeTableHasTableName && finishFirst) {
            fromTable = ctx.table_name().ident().getText();
        }

        if (!firstMergeTableHasTableName && !finishFirst) {
            toTable = ctx.table_name().ident().getText();
        }
        finishFirst = true;

        return visitChildren(ctx);
    }

    public static void main(String[] args) throws IOException {
        String inputFile="src/test/java/TableFigureTest.txt";
        if(args.length>0) inputFile = args[0];
        InputStream is = System.in;
        if(inputFile !=null)is= new FileInputStream(inputFile);
        ANTLRInputStream input = new ANTLRInputStream(is);
        HplsqlLexer lexer = new HplsqlLexer(input);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        HplsqlParser parser = new HplsqlParser(tokenStream);


        /*
            自定义visitor全部遍历
         */
        ParseTree tree = parser.program();
        TestVisitor visitor = new TestVisitor();
        visitor.visit(tree);
        visitor.printResult();
    }

    public void printResult() {
        System.out.println("t " + toTable);
        System.out.println("f " + fromTable);
    }
}
