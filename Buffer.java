
import java.util.HashMap;

public class Buffer
{
	public int noOfRows;
	public HashMap<Integer, String[]> content;
	public int[] format;
	public String[] columnName;

	public Buffer()
	{
		noOfRows = 0;
		content = new HashMap<Integer, String[]>();
	}

	public void add(int rowid, String[] val)
	{
		content.put(rowid, val);
		noOfRows = noOfRows + 1;
	}

	public void adjustFormat()
	{
		for(int i = 0; i < format.length; i++)
			format[i] = columnName[i].length();
		for(String[] i : content.values()){
			for(int j = 0; j < i.length; j++)
				if(format[j] < i[j].length())
					format[j] = i[j].length();
		}
	}

	public String fix(int len, String s)
	{
		return String.format("%-"+(len+3)+"s", s);
	}

	public String line(String s,int len)
	{
		String a = "";
		for(int i=0;i<len;i++) 
		{
			a += s;
		}
		return a;
	}

	public void output(String[] col)
	{
		if(noOfRows == 0)
		{
			System.out.println();
			for(int j = 0; j < columnName.length; j++)
				System.out.print(fix(format[j], columnName[j])+"|");
			System.out.println();
			for(int l: format)
				System.out.print(line(".", l+5));
			System.out.println();
		}
		else
		{
			adjustFormat();
			if(col[0].equals("*"))
			{
				System.out.println();
				for(int j = 0; j < columnName.length; j++)
					System.out.print(fix(format[j], columnName[j])+"|");
				System.out.println();
				for(int l: format)
					System.out.print(line(".", l+5));
				System.out.println();
				for(String[] i : content.values()){
					if(i[0].equals("-10000"))
						continue;
					for(int j = 0; j < i.length; j++)
						System.out.print(fix(format[j], i[j])+"|");
					System.out.println();
				}
				System.out.println();
			}
			else
			{
				int[] control = new int[col.length];
				for(int j = 0; j < col.length; j++)
					for(int i = 0; i < columnName.length; i++)
						if(col[j].equals(columnName[i]))
							control[j] = i;
				System.out.println();
				for(int j = 0; j < control.length; j++)
					System.out.print(fix(format[control[j]], columnName[control[j]])+"|");
				System.out.println();
				for(int j = 0; j < control.length; j++)
					System.out.print(line(".", format[control[j]]+5));
				System.out.println();
				for(String[] i : content.values())
				{
					for(int j = 0; j < control.length; j++)
						System.out.print(fix(format[control[j]], i[control[j]])+"|");
					System.out.println();
				}
				System.out.println();
			}
		}
	}
}
