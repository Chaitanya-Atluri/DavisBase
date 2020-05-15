

import java.io.RandomAccessFile;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;

public class Table
{
	public static final int pageSize = 512;
	public static final String datePattern = "yyyy-MM-dd_HH:mm:ss";
	private static RandomAccessFile davisbaseMetaTables;
	private static RandomAccessFile davisbaseMetaColumns;
	
	public static void show()
	{
		String[] cols = {"table_name"};
		String[] cmp = new String[0];
		String table = "davisbase_tables";
		
		select("data/"+table+".tbl",table, cols, cmp);
	}
	
	
	public static void drop(String table,String db)
	{
		try{
			String[] cols = getColumnName(table);
			RandomAccessFile file = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			int numPages = pages(file);
			for(int page = 1; page <= numPages; page ++)
			{
				file.seek((page-1)*pageSize);
				byte type = file.readByte();
				if(type == 0x05)
					continue;
				else{
					short[] cells = Tree.getCellArray(file, page);
					int i = 0;
					for(int j = 0; j < cells.length; j++){
						long loc = Tree.getCellLoc(file, page, j);
						String[] pl = retrievePayload(file, loc);
						String tb = pl[1];
						if(!tb.equals(DavisBase.dbActive+table)){
							Tree.setCellOff(file, page, i, cells[j]);
							i++;
						}
					}
					Tree.setCellNum(file, page, (byte)i);
				}
			}
			file.close();
			file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			numPages = pages(file);
			for(int page = 1; page <= numPages; page ++){
				file.seek((page-1)*pageSize);
				byte type = file.readByte();
				if(type == 0x05)
					continue;
				else{
					short[] cells = Tree.getCellArray(file, page);
					int i = 0;
					for(int j = 0; j < cells.length; j++){
						long loc = Tree.getCellLoc(file, page, j);
						String[] pl = retrievePayload(file, loc);
						String tb = pl[1];
						if(!tb.equals(DavisBase.dbActive+table))
						{
							Tree.setCellOff(file, page, i, cells[j]);
							i++;
						}
					}
					Tree.setCellNum(file, page, (byte)i);
				}
			}
			file.close();
			
			//Delete table files
			File dropTable = new File("data",table+".tbl");
			dropTable.delete();	
			file.close();		
		}
		catch(Exception e)
		{
			System.out.println("Error at drop table");
			System.out.println(e);
		}

	}
	
	
	
	public static String[] retrievePayload(RandomAccessFile file, long loc)
	{
		String[] payload = new String[0];
		try{
			Long tmp;
			SimpleDateFormat formater = new SimpleDateFormat (datePattern);

			// get stc
			file.seek(loc);
			int plsize = file.readShort();
			int key = file.readInt();
			int num_cols = file.readByte();
			byte[] stc = new byte[num_cols];
			int temp = file.read(stc);
			payload = new String[num_cols+1];
			payload[0] = Integer.toString(key);
			// get payLoad
			for(int i=1; i <= num_cols; i++){
				switch(stc[i-1]){
					case 0x00:  payload[i] = Integer.toString(file.readByte());
								payload[i] = "null";
								break;

					case 0x01:  payload[i] = Integer.toString(file.readShort());
								payload[i] = "null";
								break;

					case 0x02:  payload[i] = Integer.toString(file.readInt());
								payload[i] = "null";
								break;

					case 0x03:  payload[i] = Long.toString(file.readLong());
								payload[i] = "null";
								break;

					case 0x04:  payload[i] = Integer.toString(file.readByte());
								break;

					case 0x05:  payload[i] = Integer.toString(file.readShort());
								break;

					case 0x06:  payload[i] = Integer.toString(file.readInt());
								break;

					case 0x07:  payload[i] = Long.toString(file.readLong());
								break;

					case 0x08:  payload[i] = String.valueOf(file.readFloat());
								break;

					case 0x09:  payload[i] = String.valueOf(file.readDouble());
								break;

					case 0x0A:  tmp = file.readLong();
								Date dateTime = new Date(tmp);
								payload[i] = formater.format(dateTime);
								break;

					case 0x0B:  tmp = file.readLong();
								Date date = new Date(tmp);
								payload[i] = formater.format(date).substring(0,10);
								break;

					default:    int len = new Integer(stc[i-1]-0x0C);
								byte[] bytes = new byte[len];
								for(int j = 0; j < len; j++)
									bytes[j] = file.readByte();
								payload[i] = new String(bytes);
								break;
				}
			}

		}
		catch(Exception e)
		{
			System.out.println("Error at retrievePayload");
		}

		return payload;
	}


	public static void createTable(String table, String[] col)
	{
		try{	
			//Creating a file
			File catalog = new File("data/");
			
			catalog.mkdir();
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			file.setLength(pageSize);
			file.seek(0);
			file.writeByte(0x0D);
			file.close();
			
			file = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			int numPages = pages(file);
			int page = 1;
			for(int p = 1; p <= numPages; p++)
			{
				int rm = Tree.getRightMost(file, p);
				if(rm == 0)
					page = p;
			}
			int[] keyArray = Tree.getKeyArray(file, page);
			int l = keyArray[0];
			for(int i = 0; i < keyArray.length; i++)
				if(l < keyArray[i])
					l = keyArray[i];
			file.close();
			String[] values = {Integer.toString(l+1), DavisBase.dbActive+table};
			insertInto("davisbase_tables", values);

			RandomAccessFile cfile = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {};
			filter(cfile, cmp, columnName, buffer);
			l = buffer.content.size();

			for(int i = 0; i < col.length; i++){
				l = l + 1;
				String[] token = col[i].split(" ");
				String n = "YES";
				if(token.length > 2)
					n = "NO";
				String col_name = token[0];
				String dt = token[1].toUpperCase();
				String pos = Integer.toString(i+1);
				String[] v = {Integer.toString(l), DavisBase.dbActive+table, col_name, dt, pos, n};
				insertInto("davisbase_columns", v);
			}
			cfile.close();
			file.close();
		}catch(Exception e){
			System.out.println("Error at create table");
			e.printStackTrace();
		}
	}


	public static void insertInto(RandomAccessFile file, String table, String[] values)
	{
		String[] dtype = getDataType(table);
		String[] nullable = getNullable(table);

		for(int i = 0; i < nullable.length; i++)
			if(values[i].equals("null") && nullable[i].equals("NO")){
				System.out.println("NULL value constraint violation, try non null value again");
				System.out.println();
				return;
			}


		int key = new Integer(values[0]);
		int page = searchKey(file, key);
		if(page != 0)
			if(Tree.hasKey(file, page, key))
			{
				System.out.println("Uniqueness constraint violation, Provide different value in first field to accept");
				System.out.println();
				return;
			}
		if(page == 0)
			page = 1;


		byte[] stc = new byte[dtype.length-1];
		short plSize = (short) payloadSize(table, values, stc);
		int cellSize = plSize + 6;
		int offset = Tree.checkLeafSpace(file, page, cellSize);

		if(offset != -1)
		{
			Tree.insertLeafCell(file, page, offset, plSize, key, stc, values,table);
		}
		else
		{
			Tree.splitLeaf(file, page);
			insertInto(file, table, values);
		}
	}

	public static void insertInto(String table, String[] values)
	{
		try
		{
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			insertInto(file, table, values);
			file.close();

		}
		catch(Exception e)
		{
			System.out.println("Error in inserting the data");
			e.printStackTrace();
		}
	}

	public static int payloadSize(String table, String[] vals, byte[] stc)
	{
		String[] dataType = getDataType(table);
		int size = 1;
		size = size + dataType.length - 1;
		for(int i = 1; i < dataType.length; i++){
			byte tmp = stcCode(vals[i], dataType[i]);
			stc[i - 1] = tmp;
			size = size + feildLength(tmp);
		}
		return size;
	}

	public static short feildLength(byte stc)
	{
		switch(stc)
		{
			case 0x00: return 1;
			case 0x01: return 2;
			case 0x02: return 4;
			case 0x03: return 8;
			case 0x04: return 1;
			case 0x05: return 2;
			case 0x06: return 4;
			case 0x07: return 8;
			case 0x08: return 4;
			case 0x09: return 8;
			case 0x0A: return 8;
			case 0x0B: return 8;
			default:   return (short)(stc - 0x0C);
		}
	}

	public static byte stcCode(String val, String dataType)
	{
		if(val.equals("null"))
		{
			switch(dataType)
			{
				case "TINYINT":     return 0x00;
				case "SMALLINT":    return 0x01;
				case "INT":			return 0x02;
				case "BIGINT":      return 0x03;
				case "REAL":        return 0x02;
				case "DOUBLE":      return 0x03;
				case "DATETIME":    return 0x03;
				case "DATE":        return 0x03;
				case "TEXT":        return 0x03;
				default:			return 0x00;
			}							
		}
		else
		{
			switch(dataType)
			{
				case "TINYINT":     return 0x04;
				case "SMALLINT":    return 0x05;
				case "INT":			return 0x06;
				case "BIGINT":      return 0x07;
				case "REAL":        return 0x08;
				case "DOUBLE":      return 0x09;
				case "DATETIME":    return 0x0A;
				case "DATE":        return 0x0B;
				case "TEXT":        return (byte)(val.length()+0x0C);
				default:			return 0x00;
			}
		}
	}

	public static int searchKey(RandomAccessFile file, int key)
	{
		int val = 1;
		try{
			int numPages = pages(file);
			for(int page = 1; page <= numPages; page++){
				file.seek((page - 1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x0D){
					int[] keys = Tree.getKeyArray(file, page);
					if(keys.length == 0)
						return 0;
					int rm = Tree.getRightMost(file, page);
					if(keys[0] <= key && key <= keys[keys.length - 1]){
						return page;
					}else if(rm == 0 && keys[keys.length - 1] < key){
						return page;
					}
				}
			}
		}
		catch(Exception e)
		{
			System.out.println("Error at searchKey");
			System.out.println(e);
		}

		return val;
	}


	public static String[] getDataType(String table)
	{
		String[] dataType = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			if(!table.equals("davisbase_columns") && !table.equals("davisbase_tables"))
				table = DavisBase.dbActive+table;
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[3]);
			}
			dataType = array.toArray(new String[array.size()]);
			file.close();
			return dataType;
		}
		catch(Exception e)
		{
			System.out.println("Error in getting the data type");
			System.out.println(e);
		}
		return dataType;
	}

	public static String[] getColumnName(String table)
	{
		String[] c = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			if(!table.equals("davisbase_columns") && !table.equals("davisbase_tables"))
				table = DavisBase.dbActive+table;
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[2]);
			}
			c = array.toArray(new String[array.size()]);
			file.close();
			return c;
		}
		catch(Exception e)
		{
			System.out.println("Error in getting the column name");
			System.out.println(e);
		}
		return c;
	}
	
	public static String[] getNullable(String table)
	{
		String[] n = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			Buffer buffer = new Buffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			if(!table.equals("davisbase_columns") && !table.equals("davisbase_tables"))
				table = DavisBase.dbActive+table;
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values())
			{
				array.add(i[5]);
			}
			n = array.toArray(new String[array.size()]);
			file.close();
			return n;
		}catch(Exception e){
			System.out.println("Error at getNullable");
			System.out.println(e);
		}
		return n;
	}

	public static void select(String file, String table, String[] cols, String[] cmp)
	{
		try
		{
			int i;
			Buffer buffer = new Buffer();
			String[] columnName = getColumnName(table);
			String[] type = getDataType(table);


			RandomAccessFile rFile = new RandomAccessFile(file, "rw");
			filter(rFile, cmp, columnName, type, buffer);
			buffer.output(cols);
			rFile.close();
		}
		catch(Exception e)
		{
			System.out.println("Error at select");
			System.out.println(e);
		}
	}


	public static void filter(RandomAccessFile file, String[] cmp, String[] columnName, String[] type, Buffer buffer){
		try{
			int numPages = pages(file);
			// get column_name
			for(int page = 1; page <= numPages; page++)
			{
				file.seek((page-1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x05)
					continue;
				else{
					byte numCells = Tree.getCellNum(file, page);

					for(int i=0; i < numCells; i++){
						//System.out.println("check point");
						long loc = Tree.getCellLoc(file, page, i);
						file.seek(loc+2); // seek to rowid
						int rowid = file.readInt(); // read rowid
						int num_cols = new Integer(file.readByte()); // read # of columns other than rowid

						String[] payload = retrievePayload(file, loc);

						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								payload[j] = "'"+payload[j]+"'";
						// check
						boolean check = cmpCheck(payload, rowid, cmp, columnName);

						// convert back date type
						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								payload[j] = payload[j].substring(1, payload[j].length()-1);

						if(check)
							buffer.add(rowid, payload);
					}
				}
			}

			buffer.columnName = columnName;
			buffer.format = new int[columnName.length];

		}
		catch(Exception e)
		{
			System.out.println("Error at filter");
			e.printStackTrace();
		}

	}

	public static void filter(RandomAccessFile file, String[] cmp, String[] columnName, Buffer buffer){
		try{
			int numPages = pages(file);
			// get column_name
			for(int page = 1; page <= numPages; page++){
				file.seek((page-1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x05)
					continue;
				else{
					byte numCells = Tree.getCellNum(file, page);

					for(int i=0; i < numCells; i++){
						long loc = Tree.getCellLoc(file, page, i);
						file.seek(loc+2); // seek to row_id
						int rowid = file.readInt(); // read row_id
						int num_cols = new Integer(file.readByte()); // read # of columns other than rowid
						String[] payload = retrievePayload(file, loc);

						boolean check = cmpCheck(payload, rowid, cmp, columnName);
						if(check)
							buffer.add(rowid, payload);
					}
				}
			}

			buffer.columnName = columnName;
			buffer.format = new int[columnName.length];

		}catch(Exception e){
			System.out.println("Error at filter");
			e.printStackTrace();
		}

	}

	public static int pages(RandomAccessFile file)
	{
		int num_pages = 0;
		try
		{
			num_pages = (int)(file.length()/(new Long(pageSize)));
		}
		catch(Exception e)
		{
			System.out.println("Error at IntPageMarker");
		}

		return num_pages;
	}

	public static boolean cmpCheck(String[] payload, int rowid, String[] cmp, String[] columnName)
	{

		boolean check = false;
		if(cmp.length == 0)
		{
			check = true;
		}
		else{
			int colPos = 1;
			for(int i = 0; i < columnName.length; i++){
				if(columnName[i].equals(cmp[0])){
					colPos = i + 1;
					break;
				}
			}
			String opt = cmp[1];
			String val = cmp[2];
			if(colPos == 1)
			{
				switch(opt)
				{
					case "=": if(rowid == Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">": if(rowid > Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case "<": if(rowid < Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">=": if(rowid >= Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;	
							  break;
					case "<=": if(rowid <= Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;	
							  break;
					case "<>": if(rowid != Integer.parseInt(val))  // TODO: check the operator
								check = true;
							  else
							  	check = false;	
							  break;						  							  							  							
				}
			}else{
				if(val.equals(payload[colPos-1]))
					check = true;
				else
					check = false;
			}
		}
		return check;
	}

	public static void initializeDataStore() 
	{
		try {
			davisbaseMetaTables = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			davisbaseMetaTables.setLength(pageSize);
			davisbaseMetaTables.seek(0);
			davisbaseMetaTables.write(0x0D);
			davisbaseMetaTables.write(0x02);
			int[] offset=new int[2];
			int size1=24;
			int size2=25;
			offset[0]=pageSize-size1;
			offset[1]=offset[0]-size2;
			davisbaseMetaTables.writeShort(offset[1]);
			davisbaseMetaTables.writeInt(0);
			davisbaseMetaTables.writeInt(10);
			davisbaseMetaTables.writeShort(offset[1]);
			davisbaseMetaTables.writeShort(offset[0]);
			davisbaseMetaTables.seek(offset[0]);
			davisbaseMetaTables.writeShort(20);
			davisbaseMetaTables.writeInt(1); 
			davisbaseMetaTables.writeByte(1);
			davisbaseMetaTables.writeByte(28);
			davisbaseMetaTables.writeBytes("davisbase_tables");
			davisbaseMetaTables.seek(offset[1]);
			davisbaseMetaTables.writeShort(21);
			davisbaseMetaTables.writeInt(2); 
			davisbaseMetaTables.writeByte(1);
			davisbaseMetaTables.writeByte(29);
			davisbaseMetaTables.writeBytes("davisbase_columns");
		}
		catch (Exception e) 
		{
			System.out.println("Unable to create the database_tables file");
			System.out.println(e);
		}
		
		try 
		{
			davisbaseMetaColumns = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			davisbaseMetaColumns.setLength(pageSize);
			davisbaseMetaColumns.seek(0);       
			davisbaseMetaColumns.writeByte(0x0D); 
			davisbaseMetaColumns.writeByte(0x08); 
			int[] offset=new int[10];
			offset[0]=pageSize-43;
			offset[1]=offset[0]-47;
			offset[2]=offset[1]-44;
			offset[3]=offset[2]-48;
			offset[4]=offset[3]-49;
			offset[5]=offset[4]-47;
			offset[6]=offset[5]-57;
			offset[7]=offset[6]-49;
			offset[8]=offset[7]-49;
			davisbaseMetaColumns.writeShort(offset[8]);
			davisbaseMetaColumns.writeInt(0); 
			davisbaseMetaColumns.writeInt(0); 
			// cell array
			for(int i=0;i<9;i++)
				davisbaseMetaColumns.writeShort(offset[i]);

			// inserting data
			davisbaseMetaColumns.seek(offset[0]);
			davisbaseMetaColumns.writeShort(33); 
			davisbaseMetaColumns.writeInt(1); 
			davisbaseMetaColumns.writeByte(5);
			davisbaseMetaColumns.writeByte(28);
			davisbaseMetaColumns.writeByte(17);
			davisbaseMetaColumns.writeByte(15);
			davisbaseMetaColumns.writeByte(4);
			davisbaseMetaColumns.writeByte(14);
			davisbaseMetaColumns.writeBytes("davisbase_tables");
			davisbaseMetaColumns.writeBytes("rowid"); 
			davisbaseMetaColumns.writeBytes("INT");
			davisbaseMetaColumns.writeByte(1); 
			davisbaseMetaColumns.writeBytes("NO");
			davisbaseMetaColumns.seek(offset[1]);
			davisbaseMetaColumns.writeShort(39); 
			davisbaseMetaColumns.writeInt(2); 
			davisbaseMetaColumns.writeByte(5);
			davisbaseMetaColumns.writeByte(28);
			davisbaseMetaColumns.writeByte(22);
			davisbaseMetaColumns.writeByte(16);
			davisbaseMetaColumns.writeByte(4);
			davisbaseMetaColumns.writeByte(14);
			davisbaseMetaColumns.writeBytes("davisbase_tables"); // 16
			davisbaseMetaColumns.writeBytes("table_name"); // 10  
			davisbaseMetaColumns.writeBytes("TEXT"); // 4
			davisbaseMetaColumns.writeByte(2); // 1
			davisbaseMetaColumns.writeBytes("NO"); // 2
			davisbaseMetaColumns.seek(offset[2]);
			davisbaseMetaColumns.writeShort(34); // 35
			davisbaseMetaColumns.writeInt(3); 
			davisbaseMetaColumns.writeByte(5);
			davisbaseMetaColumns.writeByte(29);
			davisbaseMetaColumns.writeByte(17);
			davisbaseMetaColumns.writeByte(15);
			davisbaseMetaColumns.writeByte(4);
			davisbaseMetaColumns.writeByte(14);
			davisbaseMetaColumns.writeBytes("davisbase_columns");
			davisbaseMetaColumns.writeBytes("rowid");
			davisbaseMetaColumns.writeBytes("INT");
			davisbaseMetaColumns.writeByte(1);
			davisbaseMetaColumns.writeBytes("NO");
			davisbaseMetaColumns.seek(offset[3]);
			davisbaseMetaColumns.writeShort(40); // 39
			davisbaseMetaColumns.writeInt(4); 
			davisbaseMetaColumns.writeByte(5);
			davisbaseMetaColumns.writeByte(29);
			davisbaseMetaColumns.writeByte(22);
			davisbaseMetaColumns.writeByte(16);
			davisbaseMetaColumns.writeByte(4);
			davisbaseMetaColumns.writeByte(14);
			davisbaseMetaColumns.writeBytes("davisbase_columns");
			davisbaseMetaColumns.writeBytes("table_name");
			davisbaseMetaColumns.writeBytes("TEXT");
			davisbaseMetaColumns.writeByte(2);
			davisbaseMetaColumns.writeBytes("NO");
			davisbaseMetaColumns.seek(offset[4]);
			davisbaseMetaColumns.writeShort(41); // 40
			davisbaseMetaColumns.writeInt(5); 
			davisbaseMetaColumns.writeByte(5);
			davisbaseMetaColumns.writeByte(29);
			davisbaseMetaColumns.writeByte(23);
			davisbaseMetaColumns.writeByte(16);
			davisbaseMetaColumns.writeByte(4);
			davisbaseMetaColumns.writeByte(14);
			davisbaseMetaColumns.writeBytes("davisbase_columns");
			davisbaseMetaColumns.writeBytes("column_name");
			davisbaseMetaColumns.writeBytes("TEXT");
			davisbaseMetaColumns.writeByte(3);
			davisbaseMetaColumns.writeBytes("NO");
			davisbaseMetaColumns.seek(offset[5]);
			davisbaseMetaColumns.writeShort(39);
			davisbaseMetaColumns.writeInt(6); 
			davisbaseMetaColumns.writeByte(5);
			davisbaseMetaColumns.writeByte(29);
			davisbaseMetaColumns.writeByte(21);
			davisbaseMetaColumns.writeByte(16);
			davisbaseMetaColumns.writeByte(4);
			davisbaseMetaColumns.writeByte(14);
			davisbaseMetaColumns.writeBytes("davisbase_columns");
			davisbaseMetaColumns.writeBytes("data_type");
			davisbaseMetaColumns.writeBytes("TEXT");
			davisbaseMetaColumns.writeByte(4);
			davisbaseMetaColumns.writeBytes("NO");
			davisbaseMetaColumns.seek(offset[6]);
			davisbaseMetaColumns.writeShort(49);
			davisbaseMetaColumns.writeInt(7); 
			davisbaseMetaColumns.writeByte(5);
			davisbaseMetaColumns.writeByte(29);
			davisbaseMetaColumns.writeByte(28);
			davisbaseMetaColumns.writeByte(19);
			davisbaseMetaColumns.writeByte(4);
			davisbaseMetaColumns.writeByte(14);
			davisbaseMetaColumns.writeBytes("davisbase_columns");
			davisbaseMetaColumns.writeBytes("ordinal_position");
			davisbaseMetaColumns.writeBytes("TINYINT");
			davisbaseMetaColumns.writeByte(5);
			davisbaseMetaColumns.writeBytes("NO");
			davisbaseMetaColumns.seek(offset[7]);
			davisbaseMetaColumns.writeShort(41);
			davisbaseMetaColumns.writeInt(8); 
			davisbaseMetaColumns.writeByte(5);
			davisbaseMetaColumns.writeByte(29);
			davisbaseMetaColumns.writeByte(23);
			davisbaseMetaColumns.writeByte(16);
			davisbaseMetaColumns.writeByte(4);
			davisbaseMetaColumns.writeByte(14);
			davisbaseMetaColumns.writeBytes("davisbase_columns");
			davisbaseMetaColumns.writeBytes("is_nullable");
			davisbaseMetaColumns.writeBytes("TEXT");
			davisbaseMetaColumns.writeByte(6);
			davisbaseMetaColumns.writeBytes("NO");
		}
		catch (Exception e) 
		{
			System.out.println("Unable to create the database_columns file");
			System.out.println(e);
		}
	}
}


