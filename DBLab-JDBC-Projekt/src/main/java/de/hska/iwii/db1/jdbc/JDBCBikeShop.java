package de.hska.iwii.db1.jdbc;

import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Random;

import org.postgresql.util.PSQLException;

import com.mysql.cj.protocol.Resultset;

/**
 * Diese Klasse ist die Basis für Ihre Lösung. Mit Hilfe der
 * Methode reInitializeDB können Sie die beim Testen veränderte
 * Datenbank wiederherstellen.
 */
public class JDBCBikeShop {
    private Connection connection;
    
    JDBCBikeShop(String host, int port, String database, String username, String password) {
        connection = creatJDBCConnection(host, port, database, username, password);
        
    }
    private Connection creatJDBCConnection(String host, int port, String database, String username, String password) {
        
            String url = "jdbc:postgresql://" + host + ":" + port + "/" + database;
    
            try {
         
                Class.forName("org.postgresql.Driver");
    
           
                return DriverManager.getConnection(url, username, password);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                return null;
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
            
        }
        public void closeJDBCConnection() {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        public void printJDBCMetaData() {
            try {
                System.out.println("Host name:\""+connection.getCatalog()+"\"");
                System.out.println("Used Drivers:\""+connection.getMetaData().getDriverName()+"\"");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        ResultSet createTable( String QueryRequest) {
            try {
               return connection.createStatement().executeQuery(QueryRequest); 
                
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }
        }
        public void createparts( ){

        }
        public void printTable(ResultSet table){
            try {
                String top = String . format("%-" + (table.getMetaData().getColumnDisplaySize(1)) + "s" , table.getMetaData().getColumnName(1));
                for (int i = 2; i < (table.getMetaData().getColumnCount()+1); i++) {
                    top += "| "+ String . format("%-" +  (table.getMetaData().getColumnDisplaySize(i)) + "s" , table.getMetaData().getColumnName(i));
                
                }
                System.out.println(top);
                String type = String . format("%-" + (table.getMetaData().getColumnDisplaySize(1)) + "s" , table.getMetaData().getColumnTypeName(1));
                for (int i = 2; i < (table.getMetaData().getColumnCount()+1); i++) {
                    type += "| "+ String . format("%-" +  (table.getMetaData().getColumnDisplaySize(i)) + "s" , table.getMetaData().getColumnTypeName(i));
                
                }
                
                System.out.println(type  );
                String separator = "";
                for (int i = 1; i < (table.getMetaData().getColumnCount()); i++) {
                    separator += String.format("%-" + (table.getMetaData().getColumnDisplaySize(i))+ "s", "-").replace(' ', '-');
                    separator += "+-";
                    }
               
                 System.out.println(separator);
                
                while(table.next()) {
                    String result = "";
                    result += String.format("%-" + (table.getMetaData().getColumnDisplaySize(1)) + "s" , table.getString(table.getMetaData().getColumnName(1)));
                    
                    for (int i = 2; i < (table.getMetaData().getColumnCount()+1); i++) {
                        result += "| "+ String.format("%-" + (table.getMetaData().getColumnDisplaySize(i)) + "s" , table.getString(table.getMetaData().getColumnName(i)));
                    }
                    System.out.println(result);
                }
                System.out.println("\n");
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
    }
    public ResultSet filterCustomer(){
        return filterCustomer("");
    }
    public ResultSet  filterCustomer(String filter) {
        try {
           
            return connection.createStatement().executeQuery( "SELECT k.name AS Kunde, k.nr AS knr, l.name AS Lieferant , k.nr AS lnr " + 
            "FROM Kunde k " +
            "JOIN Lieferant l ON k.nr = l.nr " +  
            "WHERE k.NAME LIKE '%"+filter+"%';" );
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
    public void addComissionpost(int commissionId, int articleId, int amount, int price) {
        try {
            ResultSet resultSet = connection.createStatement().executeQuery("SELECT MAX(auftrnr) FROM Auftragsposten;");
            int maxId = 0;
           
            if(resultSet.next()) {
               String p = resultSet.getString(resultSet.getMetaData().getColumnName(1))+"";
               maxId = Integer.parseInt(p);
            }
            maxId+=1;
             connection.prepareStatement("INSERT INTO Auftragsposten (posnr, auftrnr, teilnr, anzahl, gesamtpreis ) VALUES ("+maxId+","+commissionId+", "+articleId+", "+amount+","+price+");").executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
          
        }
    }
    public void addComission(LocalDate date, String CustomerName, String street, int plz, String ort, int articleNumb, int amount, int maxPrice) {
        
        
        try {
            addCustomer(CustomerName, street, plz, ort, '0');
            ResultSet resultSet = connection.createStatement().executeQuery("SELECT nr FROM Kunde WHERE name LIKE '%"+CustomerName+"%';");
            int customerID = 0;
            if(resultSet.next()) {
                String p = resultSet.getString(resultSet.getMetaData().getColumnName(1))+"";
  
                customerID = Integer.parseInt(p);
            }
            int StaffId = 0;
            resultSet = connection.createStatement().executeQuery("SELECT COUNT(persnr) FROM Auftrag;");
            if(resultSet.next()) {
                String p = resultSet.getString(resultSet.getMetaData().getColumnName(1))+"";
                StaffId = Integer.parseInt(p);
            }
            StaffId = new Random().nextInt(StaffId-1)+1;
            resultSet = connection.createStatement().executeQuery("SELECT Max(auftrnr) FROM Auftrag;");
            int auftrnr = 0;
            if(resultSet.next()) {
                String p = resultSet.getString(resultSet.getMetaData().getColumnName(1))+"";
                auftrnr= Integer.parseInt(p);
            }
            auftrnr+=1;
                    
        addComission(customerID, date, articleNumb, amount, maxPrice);
    } catch (SQLException e) {
        e.printStackTrace();
    }
    }   
    public void addComission(int customerID,LocalDate date,  int articleNumb, int amount, int maxPrice) {
        try {
            int StaffId = 0;
            ResultSet resultSet = connection.createStatement().executeQuery("SELECT COUNT(persnr) FROM Auftrag;");
            int count = 0;
            if(resultSet.next()) {
                String p = resultSet.getString(resultSet.getMetaData().getColumnName(1))+"";
                count = Integer.parseInt(p);
            }
            StaffId = new Random().nextInt(count-1)+1;

            resultSet = connection.createStatement().executeQuery("SELECT Max(auftrnr) FROM Auftrag;");
            int auftrnr = 0;
            if(resultSet.next()) {
                String p = resultSet.getString(resultSet.getMetaData().getColumnName(1))+"";
                auftrnr= Integer.parseInt(p);
            }
            auftrnr+=1;
            System.out.println("Insert Auftrag:"+auftrnr+" "+date+" "+customerID+" "+StaffId+"");
            connection.prepareStatement("INSERT INTO Auftrag (auftrnr, datum, kundnr, persnr) VALUES ('"+auftrnr+"','"+date+"', "+customerID+", "+StaffId+");").executeUpdate();
           
            addComissionpost(auftrnr, articleNumb, amount, maxPrice);
             } catch (SQLException e) {
                e.printStackTrace();
            
        }
        
    }

    
    public ResultSet addCustomer(String name, String strasse, int plz,  String ort, char sperre) {
        try {
            
            ResultSet resultSet = connection.createStatement().executeQuery("SELECT nr FROM Kunde ORDER BY nr DESC;");
            int maxId = 0;
           
            if(resultSet.next()) {
               String p = resultSet.getString(resultSet.getMetaData().getColumnName(1))+"";
               maxId = Integer.parseInt(p);
            }
            maxId+=1;

            
             connection.prepareStatement("INSERT INTO Kunde (nr, name, strasse, plz, ort, sperre) VALUES ('"+maxId+"','"+name+"', '"+strasse+"', "+plz+", '"+ort+"', '"+sperre+"');").executeUpdate();
            
             return null;
            
            } catch (SQLException e) {
                e.printStackTrace();
               
            return null;
        }
    }
        public static void main(String[] args) {
            String host = "localhost";
            int port = 5432;
            String database = "DB1";
            String username = "admin";
            String password = "admin";
            
            JDBCBikeShop jdbcBikeShop = new JDBCBikeShop(host, port, database, username, password);
            jdbcBikeShop.printJDBCMetaData();
            ResultSet table = jdbcBikeShop.createTable( "SELECT persnr, name, ort, aufgabe FROM personal;");
            jdbcBikeShop.printTable(table);

            table = jdbcBikeShop.createTable("SELECT * FROM kunde;");
            jdbcBikeShop.printTable(table);

            jdbcBikeShop.printTable(jdbcBikeShop.filterCustomer());

            table = jdbcBikeShop.addCustomer("NextBike","Niemandsland 13",79211, "Fucking", '0');
            table = jdbcBikeShop.createTable("SELECT * FROM kunde;");
            jdbcBikeShop.printTable(table);
            jdbcBikeShop.reInitializeDB(jdbcBikeShop.getConnection());

            jdbcBikeShop.addComission(LocalDate.now(),"notbike","Straße 3",76131,"Nicht Deutschland",200001,20,69);     
            table = jdbcBikeShop.createTable("SELECT * FROM kunde;");
            jdbcBikeShop.printTable(table);
            table = jdbcBikeShop.createTable("SELECT * FROM Auftrag;");
            jdbcBikeShop.printTable(table);
            table = jdbcBikeShop.createTable("SELECT * FROM Auftragsposten;");
            jdbcBikeShop.printTable(table);
            
            
            
          

            jdbcBikeShop.reInitializeDB(jdbcBikeShop.getConnection());
            jdbcBikeShop.closeJDBCConnection();
        }
    
    
    public Connection getConnection() {
            return connection;
        }
    /**
     * Stellt die Datenbank aus der SQL-Datei wieder her.
     * - Alle Tabllen mit Inhalt ohne Nachfrage löschen.
     * - Alle Tabellen wiederherstellen.
     * - Tabellen mit Daten füllen.
     * <p>
     * Getestet mit MsSQL 12, MySql 8.0.8, Oracle 11g, Oracle 18 XE, PostgreSQL 14.
     * <p>
     * Das entsprechende Sql-Skript befindet sich im Ordner ./sql im Projekt.
     * @param connection Geöffnete Verbindung zu dem DBMS, auf dem die
     * 					Bike-Datenbank wiederhergestellt werden soll. 
     */
    public void reInitializeDB(Connection connection) {
        try {
            System.out.println("\nInitializing DB.");
            connection.setAutoCommit(true);
            String productName = connection.getMetaData().getDatabaseProductName();
            boolean isMsSql = productName.equals("Microsoft SQL Server");
            Statement statement = connection.createStatement();
            int numStmts = 0;
            
            // Liest den Inhalt der Datei ein.
            String[] fileContents = new String(Files.readAllBytes(Paths.get("DBLab-JDBC-Projekt/sql/Bike.sql")),
					StandardCharsets.UTF_8).split(";");
            
            for (String sqlString : fileContents) {
                try {
                	// Microsoft kenn den DATE-Operator nicht.
                    if (isMsSql) {
                        sqlString = sqlString.replace(", DATE '", ", '");
                    }
                    statement.execute(sqlString);
                    System.out.print((++numStmts % 80 == 0 ? "/\n" : "."));
                } catch (SQLException e) {
                    System.out.print("\n" + sqlString.replace('\n', ' ').trim() + ": ");
                    System.out.println(e.getMessage());
                }
            }
            statement.close();
            System.out.println("\nBike database is reinitialized on " + productName +
                    "\nat URL " + connection.getMetaData().getURL()
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
