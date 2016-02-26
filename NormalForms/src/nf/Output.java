package nf;

import java.io.FileWriter;
import java.io.IOException;

public class Output {

	static String ofilename = "NF.txt";
	
	public static void createFile() throws IOException {
		FileWriter writer = new FileWriter(ofilename);
		writer.write("Table \t Form \t Complies \t Explanation\n");
		writer.close();
		return;
	}
	
	public static void writeResult(String str) throws IOException {
		FileWriter writer = new FileWriter(ofilename, true);
		writer.write(str);
		writer.close();
	}
}
