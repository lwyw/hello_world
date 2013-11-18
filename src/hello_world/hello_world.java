package hello_world;
import java.io.*;
import java.sql.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gibello.zql.ParseException;
import org.gibello.zql.ZQuery;
import org.gibello.zql.ZStatement;
import org.gibello.zql.ZUpdate;
import org.gibello.zql.ZqlParser;

public class hello_world {

	public static void main(String[] args) throws SQLException, IOException {
		// TODO Auto-generated method stub
		//MySQL connection
		MySQLConnector m = new MySQLConnector("root", "", "localhost", "tpcc");
		m.Connect();
		//Log file connection
		MySQLLogReader l = new MySQLLogReader("/home/parallels/eecs584/genlog_1117125945.txt");
		l.Open();
		//Output file connection
		FileWriter fstream = new FileWriter("/home/parallels/eecs584/output.txt", false);
        BufferedWriter out = new BufferedWriter(fstream);
        
        //initialize values
      	String q = "";
		String[][] distribution = new String[m.GetSumRows()][2];		
		int counter = 500;
		
		while((q = l.ReadNextQuery("update")) != null && counter > 0) {
			//parse query
			try {
				ByteArrayInputStream sql;
				ZqlParser par;
				ZStatement st;
				sql = new ByteArrayInputStream((q+";").getBytes("UTF-8"));
				par = new ZqlParser(sql);
				String where = "";
				st = par.readStatement();
				
				if (st instanceof ZQuery) {
					ZQuery zq = (ZQuery) st;
					if(zq.getWhere() != null) where = zq.getWhere().toString();
				}
				else if (st instanceof ZUpdate) {
					ZUpdate zq = (ZUpdate) st;
					if(zq.getWhere() != null) where = zq.getWhere().toString();
				}
				
				//execute explain+query
				System.out.println("EXPLAIN " + q);
				ResultSet rs = null;
				rs = m.Query("EXPLAIN " + q);
				out.write("query: "+q+"\n");	//write to output log
				out.write("where: "+where+"\n");	//write to output log
				
				//get output from explain
				while(rs.next()) {
					//output 
					out.write(rs.getString("id")+"\t");
					out.write(rs.getString("select_type")+"\t");
					out.write(rs.getString("table")+"\t");
					out.write(rs.getString("type")+"\t");
					out.write(rs.getString("possible_keys")+"\t");
					out.write(rs.getString("key")+"\t");
					out.write(rs.getString("key_len")+"\t");
					out.write(rs.getString("ref")+"\t");
					out.write(rs.getString("rows")+"\t");
					out.write(rs.getString("Extra")+"");
					out.write("\n");
					
					if (!rs.getString("id").equals("null") && m.TableExists(rs.getString("table"))) {
						//primary keys output
						ResultSet rs1 = null;
						rs1 = m.GetIDs(rs.getString("table"), where);
						int col_count = rs1.getMetaData().getColumnCount();
						while(rs1.next()) {
							String pk_str = "";
							for (int i = 1; i <= col_count; i++) pk_str = pk_str + "_" + rs1.getString(i); 
							out.write("count: "+rs.getString("table")+pk_str+"\n");
						}
					}
					
					counter--;
				}
				out.write("\n");
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				//e.printStackTrace();
				out.write("Error: Cannot parse query '" + q + "'\n");
			}
		}
		
		//disconnect
		out.close();	//close output file connection
		l.Close();		//close MySQL log file connection
		m.Disconnect();	//close MySQL database connection	
	}
}


class MySQLLogReader {
	String fn = null;
	FileReader filereader = null;
	BufferedReader bufferedreader = null;
	
	public MySQLLogReader(String fn) {
		this.fn = fn;
	}
	
	public void Open() throws FileNotFoundException {
		this.filereader = new FileReader(fn);
		this.bufferedreader = new BufferedReader(this.filereader);
	}
	
	public String ReadNextQuery() throws IOException {
		String query = null;
		while((query = this.bufferedreader.readLine())!= null && !query.toLowerCase().contains("query\t")) {
			//do nothing till the next query in the log
		}
		if(query != null) {
			int pos = query.indexOf("Query\t");
			query = query.substring(pos + "Query\t".length());
		}
		return query;
	}
	
	public String ReadNextQuery(String operation) throws IOException {
		String query = null;
		while((query = this.bufferedreader.readLine())!= null && (!query.toLowerCase().contains("query\t"+operation) || query.contains("@@"))) {
			//do nothing till the next query in the log
		}
		if(query != null) {
			int pos = query.indexOf("Query\t");
			query = query.substring(pos + "Query\t".length());
		}
		return query;
	}
	
	public String ReadNextSelectQuery() throws IOException {
		String query = null;
		while((query = this.bufferedreader.readLine())!= null && (!query.toLowerCase().contains("query\tselect") || query.contains("@@"))) {
			//do nothing till the next query in the log
		}
		if(query != null) {
			int pos = query.indexOf("Query\t");
			query = query.substring(pos + "Query\t".length());
		}
		return query;
	}
	
	public String ReadNextUpdateQuery() throws IOException {
		String query = null;
		while((query = this.bufferedreader.readLine())!= null && (!query.toLowerCase().contains("query\tupdate") || query.contains("@@"))) {
			//do nothing till the next query in the log
		}
		if(query != null) {
			int pos = query.indexOf("Query\t");
			query = query.substring(pos + "Query\t".length());
		}
		return query;
	}
	
	public void Close() throws IOException {
		if(this.bufferedreader != null) this.bufferedreader.close();
		if(this.filereader != null) this.filereader.close();
	}
}

class MySQLConnector {
	String url = null;
	String username = null;
	String password = null;
	String db = null;
	String host = null;
	
	Connection con = null;
	ResultSet rs = null;
	Statement stmt = null;
	
	Connection con1 = null;
	ResultSet rs1 = null;
	Statement stmt1 = null;
	
	public MySQLConnector(String username, String password, String host, String db) throws SQLException {
		this.url = "jdbc:mysql://"+host+"/"+db; //require MySQL connector
		this.username = username;
		this.password = password;
		this.db = db;
		this.host = host;
	}
	
	public void Connect() throws SQLException {
		this.con = DriverManager.getConnection(this.url, this.username, this.password);
		this.con1 = DriverManager.getConnection(this.url, this.username, this.password);
	}
	
	public ResultSet Query(String query) throws SQLException {
		if(rs1 != null) this.rs1.close();
		this.rs1 = null;
		if(this.stmt1 == null) this.stmt1 = this.con.createStatement();
		this.rs1 = this.stmt1.executeQuery(query);
		return this.rs1;
	}
	
	public ResultSet GetIDs(String table, String where) throws SQLException {
		if(rs != null) this.rs.close();
		this.rs = null;
		if(this.stmt == null) this.stmt = this.con.createStatement();
		
		//get primary key
		this.rs = this.GetPKColumns(table);
		String pk_str = "";
		while(this.rs.next()) pk_str = pk_str + ", " + this.rs.getString("column_name");
		pk_str = pk_str.substring(2);	//remove comma in front
		//String q = "SELECT " + pk_str + " FROM " + table;
		String q = "SELECT COUNT(*) FROM " + table;
		//use only valid columns
		String nwhere = "";
		if(!where.equals(null) && !where.equals("")) {
			//find all conditions
			Matcher m = Pattern.compile("\\([^\\(\\)]*\\)").matcher(where);
			while(m.find()) {
				Boolean exists = false;
				//check if column exists in table_name
				String[] s = m.group().replace("(","").replace(")","").split("[ <>=]+");
				if (s.length > 0)  exists = this.ColumnExists(table, s[0]); 
				if (exists) {
					if(nwhere.contains(m.group())) nwhere = nwhere + " OR " + m.group();
					else nwhere = nwhere + " AND " + m.group();
				}
				if (nwhere.matches("^ AND .*")) nwhere = nwhere.substring(5);
				else if (nwhere.matches("^ OR .*")) nwhere = nwhere.substring(4);
			}
		}
		
		if(!nwhere.equals(null) && !nwhere.equals("")) q = q + " WHERE " + nwhere;
		q = q+";";
		System.out.println(q);
		this.rs = this.stmt.executeQuery(q);
		return this.rs;
	}
	
	public ResultSet GetPKColumns(String table) throws SQLException {
		if(rs != null) this.rs.close();	//close the previous result set
		this.rs = null;
		if(this.stmt == null) this.stmt = this.con.createStatement();
		this.rs = this.stmt.executeQuery("SELECT column_name"
				+ " FROM INFORMATION_SCHEMA.COLUMNS"
				+ " WHERE TABLE_SCHEMA = '" + db + "'"
				+ " AND TABLE_NAME = '"+ table
				+ "' AND COLUMN_KEY = 'PRI';");
		return this.rs;
	}
	
	public ResultSet GetColumns(String table) throws SQLException {
		if(rs != null) this.rs.close();	//close the previous result set
		this.rs = null;
		if(this.stmt == null) this.stmt = this.con.createStatement();
		this.rs = this.stmt.executeQuery("SELECT column_name"
				+ " FROM INFORMATION_SCHEMA.COLUMNS"
				+ " WHERE TABLE_SCHEMA = '" + db
				+ "' AND TABLE_NAME = '"+ table + "';");
		return this.rs;
	}
	
	public ResultSet GetTables() throws SQLException {
		if(rs != null) this.rs.close();	//close the previous result set
		this.rs = null;
		if(this.stmt == null) this.stmt = this.con.createStatement();
		this.rs = this.stmt.executeQuery("SELECT table_name, table_rows"
				+ " FROM INFORMATION_SCHEMA.TABLES"
				+ " WHERE TABLE_SCHEMA = '" + db + "';");
		return this.rs;
	}
	
	public int GetMaxRows() throws SQLException {
		int max;
		if(this.rs != null) rs.close();	//close the previous result set
		this.rs = null;
		if(this.stmt == null) this.stmt = this.con.createStatement();
		this.rs = this.stmt.executeQuery("SELECT max(table_rows) as max_rows"
				+ " FROM INFORMATION_SCHEMA.TABLES"
				+ " WHERE TABLE_SCHEMA = '" + db + "';");
		this.rs.next();
		max = this.rs.getInt("max_rows");
		this.rs.close();
		return max;
	}
	
	public int GetSumRows() throws SQLException {
		int sum;
		if(this.rs != null) this.rs.close();	//close the previous result set
		this.rs = null;
		if(this.stmt == null) this.stmt = this.con.createStatement();
		this.rs = this.stmt.executeQuery("SELECT sum(table_rows) as sum_rows"
				+ " FROM INFORMATION_SCHEMA.TABLES"
				+ " WHERE TABLE_SCHEMA = '" + db + "';");
		this.rs.next();
		sum = this.rs.getInt("sum_rows");
		this.rs.close();
		return sum;
	}
	
	public Boolean TableExists(String table) throws SQLException {
		Boolean exists = false;
		if(rs != null) rs.close();	//close the previous result set
		this.rs = null;
		if(this.stmt == null) this.stmt = this.con.createStatement();
		rs = this.stmt.executeQuery("SELECT IF(EXISTS(SELECT *"
				+ " FROM INFORMATION_SCHEMA.COLUMNS"
				+ " WHERE TABLE_SCHEMA = '" + db
				+ "' AND TABLE_NAME = '"+ table + "'), TRUE, FALSE);");
		rs.next();
		exists = rs.getBoolean(1);
		rs.close();
		return exists;
	}
	
	public Boolean ColumnExists(String table, String column) throws SQLException {
		Boolean exists = false;
		if(rs != null) rs.close();	//close the previous result set
		this.rs = null;
		if(this.stmt == null) this.stmt = this.con.createStatement();
		rs = this.stmt.executeQuery("SELECT IF(EXISTS(SELECT *"
				+ " FROM INFORMATION_SCHEMA.COLUMNS"
				+ " WHERE TABLE_SCHEMA = '" + db + "'"
				+ " AND TABLE_NAME = '"+ table + "'"
				+ " AND COLUMN_NAME = '"+ column + "'"
				+ "), TRUE, FALSE);");
		rs.next();
		exists = rs.getBoolean(1);
		rs.close();
		return exists;
	}
	
	//close all connections
	public void Disconnect() throws SQLException {
		if(this.rs != null) this.rs.close();
		if(this.stmt != null) this.stmt.close();
		if(this.con != null) this.con.close();
		
		if(this.rs1 != null) this.rs1.close();
		if(this.stmt1 != null) this.stmt1.close();
		if(this.con1 != null) this.con1.close();
	}
}