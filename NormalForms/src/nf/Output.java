package nf;
/**********************************************************************************************/
/* COSC6340: Database Systems                                                                 */
/* Project: Discovering Functional Dependencies and Certifying Normal Forms with SQL Queries  */
/* Project team: Sajiva Pradhan (1007766), Xiang Xu (1356333)                                 */
/**********************************************************************************************/

import java.io.FileWriter;
import java.io.IOException;

// Print the output in text file
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
