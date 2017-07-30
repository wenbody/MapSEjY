package application;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SEGYCore{
	private String FilePath;
	private double rev;
	private long TraceNum;
	private byte DataFormat;
	private boolean isEBCDIC;
	private static short[] EBCtoASC = {
			0x00,0x01,0x02,0x03,0x9C,0x09,0x86,0x7F,
		    0x97,0x8D,0x8E,0x0B,0x0C,0x0D,0x0E,0x0F,
		    0x10,0x11,0x12,0x13,0x9D,0x85,0x08,0x87,
		    0x18,0x19,0x92,0x8F,0x1C,0x1D,0x1E,0x1F,
		    0x80,0x81,0x82,0x83,0x84,0x0A,0x17,0x1B,
		    0x88,0x89,0x8A,0x8B,0x8C,0x05,0x06,0x07,
		    0x90,0x91,0x16,0x93,0x94,0x95,0x96,0x04,
		    0x98,0x99,0x9A,0x9B,0x14,0x15,0x9E,0x1A,
		    0x20,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
		    0x00,0x00,0xA2,0x2E,0x3C,0x28,0x2B,0x7C,
		    0x26,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
		    0x00,0x00,0x21,0x24,0x2A,0x29,0x3B,0xAC,
		    0x2D,0x2F,0x00,0x00,0x00,0x00,0x00,0x00,
		    0x00,0x00,0xA6,0x2C,0x25,0x5F,0x3E,0x3F,
		    0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
		    0x00,0x60,0x3A,0x23,0x40,0x27,0x3D,0x22,
		    0x00,0x61,0x62,0x63,0x64,0x65,0x66,0x67,
		    0x68,0x69,0x00,0x00,0x00,0x00,0x00,0x00,
		    0x00,0x6A,0x6B,0x6C,0x6D,0x6E,0x6F,0x70,
		    0x71,0x72,0x00,0x00,0x00,0x00,0x00,0x00,
		    0x00,0x7E,0x73,0x74,0x75,0x76,0x77,0x78,
		    0x79,0x7A,0x00,0x00,0x00,0x00,0x00,0x00,
		    0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
		    0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
		    0x7B,0x41,0x42,0x43,0x44,0x45,0x46,0x47,
		    0x48,0x49,0x00,0x00,0x00,0x00,0x00,0x00,
		    0x7D,0x4A,0x4B,0x4C,0x4D,0x4E,0x4F,0x50,
		    0x51,0x52,0x00,0x00,0x00,0x00,0x00,0x00,
		    0x5C,0x00,0x53,0x54,0x55,0x56,0x57,0x58,
		    0x59,0x5A,0x00,0x00,0x00,0x00,0x00,0x00,
		    0x30,0x31,0x32,0x33,0x34,0x35,0x36,0x37,
		    0x38,0x39,0x00,0x00,0x00,0x00,0x00,0x9F
	};
	public SEGYCore(){
		this.setFilePath(null);
	}
	public SEGYCore(String filePath){
		this.setFilePath(filePath);
		try {
			FileInputStream fis = new FileInputStream(filePath);
			DataInputStream dis = new DataInputStream(fis);
			if("c3".equals(Integer.toHexString(dis.readByte()&0xFF))){
				this.setIsEBCDIC(true);
			}else{
				this.setIsEBCDIC(false);
			}			
			dis.skipBytes(3219);
			byte[] DataRec = new byte[2];
			dis.readFully(DataRec);
			this.setTraceNum(ByteBuffer.wrap(DataRec).order(ByteOrder.BIG_ENDIAN).getLong());
			dis.skipBytes(2);
			dis.readFully(DataRec);
			this.setDataFormat((byte)ByteBuffer.wrap(DataRec).order(ByteOrder.BIG_ENDIAN).getShort());
			dis.skipBytes(274);
			dis.readFully(DataRec);
			if("01".equals(Integer.toHexString(DataRec[0]&0xFF))&&"00".equals(Integer.toHexString(DataRec[1]&0xFF))){
				this.setRev(1);
			}else if("02".equals(Integer.toHexString(DataRec[0]&0xFF))&&"00".equals(Integer.toHexString(DataRec[1]&0xFF))){
				this.setRev(2.0);
			}else{
				this.setRev(ByteBuffer.wrap(DataRec).order(ByteOrder.BIG_ENDIAN).getInt());
			}			
			dis.close();
			fis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	public double getRev() {
		return rev;
	}
	public void setRev(double rev) {
		this.rev = rev;
	}
	public long getTraceNum() {
		return TraceNum;
	}
	public void setTraceNum(long traceNum) {
		this.TraceNum = traceNum;
	}
	public byte getDataFormat() {
		return DataFormat;
	}
	public void setDataFormat(byte dataFormat) {
		DataFormat = dataFormat;
	}
	public boolean isEBCDIC() {
		return isEBCDIC;
	}
	public void setIsEBCDIC(boolean isEBCDIC) {
		this.isEBCDIC = isEBCDIC;
	}
	public String getFilePath() {
		return FilePath;
	}
	public void setFilePath(String filePath) {
		this.FilePath = filePath;
	}
	public short[] getTextualFileHeader() {
		byte[] SEGYhead = new byte[3200];
		try {
			FileInputStream fis = new FileInputStream(this.FilePath);
			DataInputStream dis = new DataInputStream(fis);			
			dis.readFully(SEGYhead);			
			dis.close();
			fis.close();			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return SEGYtoASCII(SEGYhead);	
	}
	public int[] getBinaryFileHeader() {
		byte[] BNYhead = new byte[400];
		try {
			FileInputStream fis = new FileInputStream(this.FilePath);
			DataInputStream dis = new DataInputStream(fis);
			dis.skipBytes(3200);
			dis.readFully(BNYhead);			
			dis.close();
			fis.close();			
		} catch (IOException e) {
			e.printStackTrace();
		}
		return BNYtoValue(BNYhead);	
	}
	public short[] SEGYtoASCII(byte[] SEGYHead) {
		short[] SegyHeadValue=new short[3200];
		if(this.isEBCDIC){
			for (int i = 0;i < 3200;i++){
				SegyHeadValue[i] = EBCtoASC[Byte.toUnsignedInt(SEGYHead[i])];
		    }
		}else{
			for (int i = 0;i < 3200;i++){
				SegyHeadValue[i] = SEGYHead[i];
		    }
		}
	    return SegyHeadValue;
	}
	public int[] BNYtoValue(byte[] BNYHead) {
		int[] BnyValue = new int[30];
		byte[] FirBNYHead = new byte[4];
		byte[] SecBNYHead = new byte[2];
		int i;
		int loc=0;
		for (i = 0;i < 3;i++){
			FirBNYHead[0]=BNYHead[loc];
			FirBNYHead[1]=BNYHead[loc+1];
			FirBNYHead[2]=BNYHead[loc+2];
			FirBNYHead[3]=BNYHead[loc+3];
			BnyValue[i] = ByteBuffer.wrap(FirBNYHead).order(ByteOrder.BIG_ENDIAN).getInt();
			loc+=4;
	    }
		loc=12;
		while(i<27){
			SecBNYHead[0]=BNYHead[loc];
			SecBNYHead[1]=BNYHead[loc+1];
			BnyValue[i] = ByteBuffer.wrap(SecBNYHead).order(ByteOrder.BIG_ENDIAN).getShort();
			loc+=2;
			i++;
		}
		loc=300;
		while(i<30){
			SecBNYHead[0]=BNYHead[loc];
			SecBNYHead[1]=BNYHead[loc+1];
			BnyValue[i] = ByteBuffer.wrap(SecBNYHead).order(ByteOrder.BIG_ENDIAN).getShort();
			loc+=2;
			i++;
		}	
	    return BnyValue;
	}
	public void SEGYtoData(){
		try {
			SEGYtoData(this.FilePath);
		} catch (Exception e) {
		}
	}
	public void SEGYtoData(String filePath) throws Exception{
		FileInputStream fis = new FileInputStream(filePath);
		DataInputStream dis = new DataInputStream(fis);
		byte[] TREhead = new byte[240];
		byte[] TRESize = new byte[2];
	    int cdp,inline,ntrace=0;
	    int TreLength;
	    dis.skipBytes(3600);
	    while(dis.available()>0){	
	    	dis.readFully(TREhead);		    	
	    	TRESize[0]=TREhead[114];
	    	TRESize[1]=TREhead[115];
	    	TreLength = ByteBuffer.wrap(TRESize).order(ByteOrder.BIG_ENDIAN).getShort();
			byte[] TreData = new byte[TreLength*4];			
		    dis.readFully(TreData);
		    cdp=627 + ntrace / (1852 - 1189);
		    inline=1189 + ntrace % (1852 - 1189);
		    toTREdata1(TreData,TreLength,cdp,inline);
		    ntrace++;
	    }
	    dis.close();
	}
	
	private void toTREdata1(byte[] buf,int num,int cdp,int inline) throws SQLException{
		double result;
		Statement stmt = null;
		Connection con;
		con = DriverManager.getConnection("jdbc:phoenix:localhost:2181");
		stmt = con.createStatement();			
		for (int i = 0; i < num; i++){
			int sign = (buf[4 * i] >> 7) & 0x01;
		    int exp = buf[4 * i] & 0x7f;
		    exp = exp - 64;
		    int frac = ((buf[4 * i + 1] << 16) & 0x00ff0000)
				     | ((buf[4 * i + 2] << 8) & 0x0000ff00)
				     | (buf[4 * i + 3] & 0x000000ff) & 0x00ffffff;
		    if (frac == 0){
		    	result=0;
			    continue;
			}   
		    result = (1 - 2 * sign) * frac * Math.pow(2, 4 * exp - 24);
		    stmt.executeUpdate("UPSERT INTO DATA2 VALUES("+cdp+","+inline+","+(i+1)+","+result+")");
		}
		con.commit();
		con.close();
	}

	
}
