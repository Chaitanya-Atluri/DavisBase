import java.io.File;
import java.util.Scanner;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
public class DavisBase {

	static String prompt = "davisql> ";
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	static String dbActive = "";
	
	public static void databaseInit()
	{
		try {
			File data = new File("data");//Create meta tables
			if (!data.exists()) {
				data.mkdir();
			}
			File dbCatalog = new File("data");
			if (dbCatalog.mkdir()) {
				System.out.println("System directory 'data' doesn't exit, Initializing catalog!");
				Table.initializeDataStore();
			} else {
				boolean catalog = false;
				String meta_cols = "davisbase_columns.tbl";
				String meta_tbls = "davisbase_tables.tbl";
				String[] tableList = dbCatalog.list();

				for (int i = 0; i < tableList.length; i++) {
					if (tableList[i].equals(meta_cols))
						catalog = true;
				}
				if (!catalog) {
					System.out.println(
							"System table 'davisbase_columns.tbl' does not exit, initializing davisbase_columns...");
					System.out.println();
					Table.initializeDataStore();
				}
				catalog = false;
				for (int i = 0; i < tableList.length; i++) {
					if (tableList[i].equals(meta_tbls))
						catalog = true;
				}
				if (!catalog) {
					System.out.println(
							"System table 'davisbase_tables.tbl' does not exit, initializing davisbase_tables...");
					System.out.println();
					Table.initializeDataStore();
				}
			}
		} catch (SecurityException se) {
			System.out.println("Meta data files not created " + se);

		}

	}


	
	// To parse queries into strings
	public static void parsequery(String query) 
	{

		String[] queryTokens = query.split(" ");
		switch (queryTokens[0]) 
		{
		case "create":
			if(queryTokens[1].equals("table")){
				createParser(query);
				break;
			}
			
		case "insert":
			insertParser(query);
			break;
			
		case "select":
			selectParser(query);
			break;	
			
		case "drop":
			//selectParser(query);
			if(queryTokens[1].equals("table")){
				String dropTable = queryTokens[2];
				if(!dropTable.equals("davisbase_tables")&&!dropTable.equals("davisbase_coloumns")) {
					if (tableExists(dropTable)) {
						Table.drop(dropTable, dbActive);
						System.out.println("Table " + dropTable + " is dropped successfully.");
					} else {
						System.out.println("Table " + dropTable + " doesn't exist.");
						System.out.println();
					}
				}
				else{
					System.out.println("we cant delete " +dropTable );
				}

			}
			else{
				System.out.println("Incorrect input. Please use help command to know the supported commands");
			}
			System.out.println();
			break;

		case "show":
			String cmd = queryTokens[1];
			if(cmd.equals("tables"))
			{
				System.out.println();
				Table.show();
				System.out.println();
			}
			else
				System.out.println("Incorrect input after show \nFound "+ cmd +" instead of tables.");

			break;


		case "help":
			System.out.println();
			System.out.println("List of all DavisBase commands:");
			System.out.println(
					"\t(a)SHOW TABLES;                                                   Displays a list of all tables in DavisBase");
			System.out.println(
					"\t(b)CREATE TABLE <table_name>;                                     Creates a new table schema, i.e. a new empty table");
			System.out.println(
					"\t(c)DROP TABLE <table_name>;                                       Remove a table schema, and all of its contained data");
			System.out.println();
			System.out.println(
					"\t(a)INSERT INTO table_name [column_list] VALUES value_list;        Inserts a single record into a table");
			
			System.out.println();
			System.out.println(
					"\t(a)SELECT * FROM <table_name>;                                    Display all records in the table");
			System.out.println(
					"\t(b)SELECT * FROM <table_name> WHERE rowid = <value>;              Display records satisfying a particular condition");
			System.out.println();
			System.out.println(
					"Version;                                                                   version of database");
			System.out.println();
			System.out.println(
					"EXIT;                                                                   Exit the program");
			System.out.println();
			System.out.println(
					"HELP;                                                                   Show this help information");
			System.out.println();
			break;

		case "exit":
			System.out.println();
			break;

		case "version":
			System.out.println();
			System.out.println("DavisBase Version 1.0");
			System.out.println();
			break;

		default:
			System.out.println();
			System.out.println("Incorrect input. Please use help command to know the supported commands");
			System.out.println();
			break;

		}
	}

	public static void main(String[] args) {
		databaseInit();
	
		
		System.out.println(
				"************************************************************************************************************************");
		System.out.println("Welcome to DavisBase Version 1.0");
		System.out.println("Type \"help;\" to view all the commands supported by DavisBase");
		System.out.println(
				"************************************************************************************************************************");
	
		/* Variable to collect user input from the prompt */
		String query = "";
	
		while (!query.equals("exit")) 
		{
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			query = scanner.next().replace("\n", " ").replace("\r", "").replace("*", " * ").replace("(", " (").trim().toLowerCase();
			parsequery(query);
		}
		System.out.println("Exiting from DavisBase");
	
	}

	public static void createParser(String query) {
		String[] queryTokens = query.split(" ");
		String createTable = queryTokens[2];

		String[] create_temp = query.split(createTable);

		String col_temp = create_temp[1].trim();
		String[] create_cols = col_temp.substring(1, col_temp.length() - 1).split(",");
		for (int i = 0; i < create_cols.length; i++)
			create_cols[i] = create_cols[i].trim();
		if (tableExists(createTable))
		{
			System.out.println("Table " + createTable + " already exists.");
			System.out.println();
		}
		else {
			Table.createTable(createTable, create_cols);
			System.out.println("Table " + createTable + " created successfully.");
		}
	}

	public static void insertParser(String query) {
		try{
			String[] queryTokens = query.split(" ");
			String insert_table = queryTokens[2];
			String insert_vals = query.split("values")[1].trim();
			insert_vals = insert_vals.substring(1, insert_vals.length() - 1);
			String[] insert_values = insert_vals.split(",");
			for (int i = 0; i < insert_values.length; i++)
				insert_values[i] = insert_values[i].trim();
			if (tableExists(insert_table)) {
				RandomAccessFile file;
				try {
					file = new RandomAccessFile("data/" + insert_table + ".tbl", "rw");
					Table.insertInto(file, insert_table, insert_values);
					file.close();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException i) {
					i.printStackTrace();
				}
			}
			else
			{
				System.out.println("Table " + insert_table + " does not exist.");
				System.out.println();
			}
		}
		catch(Exception ArrayIndexOutOfBoundsException){
			System.out.println("Incorrect input. Please use help command to know the supported commands ");
		}
	}

	public static void selectParser(String query) {
		try{
			String[] select_cmp;
			String[] select_column;
			String[] select_temp = query.split("where");
			String[] selectQuery = select_temp[0].split("from");
			String selectTable = selectQuery[1].trim();
			String selectColumns = selectQuery[0].replace("select", "").trim();

			if (selectTable.equals("davisbase_tables")) {
				if (selectColumns.contains("*")) {
					select_column = new String[1];
					select_column[0] = "*";
				} else {
					select_column = selectColumns.split(",");
					for (int i = 0; i < select_column.length; i++)
						select_column[i] = select_column[i].trim();
				}
				if (select_temp.length > 1) {
					String filter = select_temp[1].trim();
					select_cmp = parserEquation(filter);
				} else {
					select_cmp = new String[0];
				}
				Table.select("data/davisbase_tables.tbl", selectTable, select_column, select_cmp);
				System.out.println();
			} else if (selectTable.equals("davisbase_columns")) {
				if (selectColumns.contains("*")) {
					select_column = new String[1];
					select_column[0] = "*";
				} else {
					select_column = selectColumns.split(",");
					for (int i = 0; i < select_column.length; i++)
						select_column[i] = select_column[i].trim();
				}
				if (select_temp.length > 1) {
					String filter = select_temp[1].trim();
					select_cmp = parserEquation(filter);
				} else {
					select_cmp = new String[0];
				}
				Table.select("data/davisbase_columns.tbl", selectTable, select_column, select_cmp);
				System.out.println();
			} else {
				if (!tableExists(selectTable)) {

					System.out.println("Table " + selectTable + " doesn't exist.");
					System.out.println("Please enter the correct table name.");
					System.out.println();
				}
			}
			if (select_temp.length > 1) {
				String filter = select_temp[1].trim();
				select_cmp = parserEquation(filter);
			} else {
				select_cmp = new String[0];
			}

			if (selectColumns.contains("*")) {
				select_column = new String[1];
				select_column[0] = "*";
			} else {
				select_column = selectColumns.split(",");
				for (int i = 0; i < select_column.length; i++)
					select_column[i] = select_column[i].trim();
			}
			if (tableExists(selectTable))
				Table.select("data/" + selectTable + ".tbl", selectTable, select_column, select_cmp);
			System.out.println();
		}
		catch(Exception ArrayIndexOutOfBoundsException){
			System.out.println("Incorrect input. Please use help command to know the supported commands ");
		}

	}


	// Check if the table exists
	public static boolean tableExists(String table) {
		boolean table_check = false;
		try {
			File user_tables = new File("data/");
			if (user_tables.mkdir()) {
				System.out.println("System directory 'data' doesn't exit, Initializing for user_data!");
			}
			File user = new File("data/",table+".tbl");
			if (user.exists()) {
				return true;}
		} catch (SecurityException se) {
			System.out.println("Unable to create data container directory" + se);
		}

		return table_check;
	}

	public static String[] parserEquation(String inputEquation)
	{
		String compareEquation[] = new String[3];
		String tmp[] = new String[2];
		if (inputEquation.contains("=")) {
			tmp = inputEquation.split("=");
			compareEquation[0] = tmp[0].trim();
			compareEquation[1] = "=";
			compareEquation[2] = tmp[1].trim();
		}

		if (inputEquation.contains(">")) {
			tmp = inputEquation.split(">");
			compareEquation[0] = tmp[0].trim();
			compareEquation[1] = ">";
			compareEquation[2] = tmp[1].trim();
		}

		if (inputEquation.contains("<")) {
			tmp = inputEquation.split("<");
			compareEquation[0] = tmp[0].trim();
			compareEquation[1] = "<";
			compareEquation[2] = tmp[1].trim();
		}

		if (inputEquation.contains(">=")) {
			tmp = inputEquation.split(">=");
			compareEquation[0] = tmp[0].trim();
			compareEquation[1] = ">=";
			compareEquation[2] = tmp[1].trim();
		}

		if (inputEquation.contains("<=")) {
			tmp = inputEquation.split("<=");
			compareEquation[0] = tmp[0].trim();
			compareEquation[1] = "<=";
			compareEquation[2] = tmp[1].trim();
		}

		if (inputEquation.contains("<>")) {
			tmp = inputEquation.split("<>");
			compareEquation[0] = tmp[0].trim();
			compareEquation[1] = "<>";
			compareEquation[2] = tmp[1].trim();
		}

		return compareEquation;
	}
}
