/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package booksample;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Matthew
 */
public class MySqlDb {
    private Connection conn;
    
    public void openConnection(String driverClass, String url, String userName, String password) throws Exception{
         Class.forName (driverClass);
          conn = DriverManager.getConnection(url, userName, password);
    }
    
    public void closeConnection() throws SQLException{
        conn.close();
    }
    
    public List<Map<String,Object>> findAllRecords(String tableName) throws SQLException{
        List<Map<String,Object>> records = new ArrayList<>();
        
        String sql = "SELECT * FROM " + tableName;
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        
        while(rs.next()){
            Map<String,Object> record = new HashMap<>();
            for(int i=1; i <= columnCount; i++){
                record.put(metaData.getCatalogName(i), rs.getObject(i));
            }
            records.add(record);
        }
        
        return records;
    }
    
    public int deleteRecord(String tableName, String columnName, Object value) throws SQLException{
        String strValue = value.toString();
        Statement stmt = conn.createStatement();
        
        String sql = "Delete From " + tableName + " WHERE " + columnName + " = ";
        
        if(value instanceof String){
            strValue = "'" + value.toString() + "'";            
        }
        
        sql += strValue;
        
        //System.out.println(sql);
        int rowsUpdated = stmt.executeUpdate(sql);
        stmt.close();
        
        return rowsUpdated;
    }
    
        public int deleteRecordWithPrepared(String tableName, String columnName, Object value) throws SQLException{
//        String strValue = value.toString();
//
//        String sql = "Delete From " + tableName + " WHERE " + columnName + " = ";
//        
//        if(value instanceof String){
//            strValue = "'" + value.toString() + "'";            
//        }
//        
//        sql += strValue;
        
        PreparedStatement pstmt = conn.prepareStatement(
                "Delete From " + tableName + " WHERE " + columnName + " = ?");
        
        pstmt.setObject(1, value);
        
        //System.out.println(sql);
        int rowsUpdated = pstmt.executeUpdate();
        pstmt.close();
        
        return rowsUpdated;
    }
    
        public int addRecord(String tableName, List columnName, List values) throws SQLException{
            PreparedStatement pstmt = null;
            int recordsAdded = 0;
            
            pstmt = buildInsertStatement(conn,tableName,columnName);
            
            final Iterator i=values.iterator();
            int index = 1;
            
            while (i.hasNext()){
                final Object obj = i.next();
                pstmt.setObject(index++, obj);
            }
            
            recordsAdded = pstmt.executeUpdate();
            
            return recordsAdded;
        }
        
        public int updateRecord(String tableName, List columnName, List values, String whereField, Object whereValue) throws SQLException{
            PreparedStatement pstmt = null;
            int recordsUpdated = 0;
            
            pstmt = buildUpdateStatement(conn,tableName,columnName,whereField);
            
            final Iterator i=values.iterator();
            int index = 1;
            
            while (i.hasNext()){
                final Object obj = i.next();
                pstmt.setObject(index++, obj);
            }
            
            pstmt.setObject(index, whereValue);
            
            recordsUpdated = pstmt.executeUpdate();
            
            return recordsUpdated;
        }
        
        
        //Review, taken for testing purposes
        private PreparedStatement buildInsertStatement(Connection conn_loc, String tableName, List columnName)
	throws SQLException {
		StringBuffer sql = new StringBuffer("INSERT INTO ");
                
		(sql.append(tableName)).append(" (");

                int i = 0;
                for(i = 0; i < columnName.size();i++){
                    (sql.append( (String)columnName.get(i) )).append(", ");
                }
                
		sql = new StringBuffer( (sql.toString()).substring( 0,(sql.toString()).lastIndexOf(", ") ) + ") VALUES (" );
		for( int j = 0; j < columnName.size(); j++ ) {
			sql.append("?, ");
		}
		final String finalSQL=(sql.toString()).substring(0,(sql.toString()).lastIndexOf(", ")) + ")";

                return conn_loc.prepareStatement(finalSQL);
	}
        
        //Review, taken for testing purposes
        private PreparedStatement buildUpdateStatement(Connection conn_loc, String tableName,
                                                       List columnName, String whereField)
	throws SQLException {
		StringBuffer sql = new StringBuffer("UPDATE ");
                
		(sql.append(tableName)).append(" SET ");
                
                int i = 0;
                for(i = 0; i < columnName.size(); i++){
                    (sql.append( (String)columnName.get(i) )).append(" = ?, ");
                }
                
                System.out.println(columnName.size() + ", " + (sql.toString()).lastIndexOf(", ") + ", " + sql.toString().length());
		sql = new StringBuffer( (sql.toString()).substring( 0,(sql.toString()).lastIndexOf(", ") ) );
		((sql.append(" WHERE ")).append(whereField)).append(" = ?");
		final String finalSQL=sql.toString();
		return conn_loc.prepareStatement(finalSQL);
	}
        
    public static void main(String[] args) throws Exception{
        MySqlDb db = new MySqlDb();
        db.openConnection("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/book", "root", "admin");
        
        List<Map<String,Object>> records = db.findAllRecords("author");
        for(Map record : records) {
            System.out.println(record);
        }
        System.out.println(db.deleteRecordWithPrepared("author", "author_id", 5));
        /*ArrayList columnNames = new ArrayList();
        columnNames.add("author_name");
        columnNames.add("date_created");
        ArrayList columnValues = new ArrayList();
        columnValues.add("Billy Newman");
        columnValues.add("2015-10-12");
        System.out.println(db.addRecord("author", columnNames, columnValues));*/
        ArrayList columnNames = new ArrayList();
        columnNames.add("author_name");
        ArrayList columnValues = new ArrayList();
        columnValues.add("Erik Krinkle");
        db.updateRecord("author", columnNames, columnValues, "author_id", 4);
        db.closeConnection();
    }
}
