package application;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class SEGYWritable implements DBWritable, Writable{	
	private int cdp=1;
	private int inline=1;
	private int time=1;
	private double record=1;
	@Override
	public void readFields(DataInput input) throws IOException {
		// TODO Auto-generated method stub
		
	}

	public int getCdp() {
		return cdp;
	}

	public int getInline() {
		return inline;
	}

	public int getTime() {
		return time;
	}

	public double getRecord() {
		return record;
	}

	@Override
	public void write(DataOutput output) throws IOException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void write(PreparedStatement pstmt) throws SQLException {
		pstmt.setInt(1, cdp);
		pstmt.setInt(2, inline);
		pstmt.setInt(3, time);
		pstmt.setDouble(4, record); 
    }
	@Override
	public void readFields(ResultSet rs) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public void setCdp(int cdp) {
		this.cdp = cdp;
	}


	public void setInline(int inline) {
		this.inline = inline;
	}

	

	public void setTime(int time) {
		this.time = time;
	}

	public void setRecord(double record) {
		this.record = record;
	}

	
	

}


