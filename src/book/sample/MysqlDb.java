/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package book.sample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author chris
 */
public class MysqlDb {
    
    private Connection conn;
    
    
    public void openConnection(String driverClass, String url, String username, String password) throws ClassNotFoundException, SQLException{
        Class.forName(driverClass);
        conn = DriverManager.getConnection(url, username, password);
    }
    
    public void closeConnection() throws SQLException{
        conn.close();
    }
    
    
    public List<Map<String, Object>> findAllRecords(String tableName) throws SQLException{
        List<Map<String, Object>> records = new ArrayList<>();
        String sql = "SELECT * FROM " + tableName;
        
        try(Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(sql);

            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();


            while(rs.next()){
                Map<String, Object> record = new HashMap<>();
                for(int i = 1; i <= columnCount; i++){
                    record.put(meta.getCatalogName(columnCount), rs.getObject(i));
                }

                records.add(record);
            }
        }
        
        return records;
    }
    
    
    public static void main(String[] args) throws Exception {
		
        MysqlDb db = new MysqlDb();
        db.openConnection("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/book", "root", "admin");
        
        List<Map<String, Object>> records = db.findAllRecords("author");
        for(Map<String, Object> record : records){
            System.out.println(record);
        }
        
        db.closeConnection();
		
    }
    
}
