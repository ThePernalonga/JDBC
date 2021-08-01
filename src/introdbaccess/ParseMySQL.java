package introdbaccess;
import java.io.*;
import java.sql.*;

/*
* args[0] = servidor da DB
* args[1] = usuario
* args[2] = senha
* args[3] = DB
* args[4] = nome da tabela (opcional)
 */

/**
 *
 * @author Andre Luiz
 * 
 * Query's em /query
 */
public class ParseMySQL {
    
    public static void main(String[] args) {
        
        if(args.length == 0 || args[0].isEmpty()){
            System.out.println("args[] empty!");
            return;
        }
        
        File dir = new File("query");
        if (!dir.exists()) {
            dir.mkdir();
        }
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e1) {
            System.out.println("Driver nao disponivel");
        }

        try {
            Connection con = DriverManager.getConnection("jdbc:mysql://" + args[0] + "/" + args[3], args[1], args[2]);

            if (args.length == 5) {
                parseCon(args[3], args[4], con);
                return;
            }
            Statement myStat = con.createStatement();

            ResultSet res = myStat.executeQuery("show tables");

            while (res.next()) {
                String tableName = res.getString(1);
                parseCon(args[3], tableName, con);
            }
        } catch (SQLException sql1){
            System.out.println("Erro no banco de dados"); 
        } 
    }
    
    public static void parseCon (String DB, String tableName, Connection con) {
        
        try{
            FileOutputStream out = new FileOutputStream("query/" + DB + "-" + tableName + ".csv");

            System.out.println(tableName);

            PreparedStatement pprStat = con.prepareStatement("select column_name from information_schema.columns"
                    + " where table_name = ?", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            pprStat.setString(1, tableName);

            ResultSet res2 = pprStat.executeQuery();

            Statement stm2 = con.createStatement();

            String query = String.format("SELECT * from `%s`", tableName);

            ResultSet res3 = stm2.executeQuery(query);

            while (res2.next()) {
                System.out.print(res2.getString(1));
                out.write(res2.getString(1).getBytes());
                if (!res2.isLast()) {
                    System.out.print(", ");
                    out.write(", ".getBytes());
                }
            }
            System.out.println();
            out.write("\n".getBytes());
            res2.beforeFirst();

            while (res3.next()) {

                while (res2.next()) {
                    System.out.print(res3.getString(res2.getString(1)));
                    if (res3.getString(res2.getString(1)) != null) {
                        out.write(res3.getString(res2.getString(1)).getBytes());
                    }

                    if (!res2.isLast()) {
                        System.out.print(", ");
                        out.write(", ".getBytes());
                    }
                }
                System.out.println();
                out.write("\n".getBytes());
                res2.beforeFirst();
            }
            out.close();
            System.out.println();
                
        } catch (FileNotFoundException e2) {
            System.out.println("File Output ERROR");
        } catch (SQLException sql2){
            System.out.println("SQL ERROR");
        } catch (IOException io1){
            System.out.println("Statement 'res2'/'res3' ERROR");
        }
    }
}
