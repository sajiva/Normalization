package nf;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


public class ArgParser {
	public static List<String> tableNames = new ArrayList<String>();
	public static List<List<String>> candidateKeys = new ArrayList<List<String>>();
	public static List<List<String>> nonKeyAttributes = new ArrayList<List<String>>();
	
	public static String txtname;
	
	// parse argument
	public static boolean parse(String[] args) {
		//check argument number should be 1
		if (1 !=args.length) {
			System.err.println("The number of argument is wrong! The syntax should be: CertifyNF database=dbnametxt");
			return false;
		}
		//find "database"
		if (args[0].contains("database=")) {
			int beginInd = args[0].indexOf("=") + 1;
			int endInd = args[0].length();
			txtname = args[0].substring(beginInd, endInd);
			System.out.println("database txt name is: "+ txtname);
		}else{
			System.err.println("The command format is wrong! The syntax should be: CertifyNF database=dbnametxt");
			return false;
		}
		return true;
	}
	
	// read file
	//@SuppressWarnings("resource")
	public static boolean readFile() {
		try {
			FileReader fileReader = new FileReader(txtname);
			BufferedReader bReader = new BufferedReader(fileReader);
			
			String line = bReader.readLine();
			int idx = 0;
			while (null != line) {
				idx ++ ;
				System.out.println("reading the " + idx + " line");
				// parse the line
				Vector<Integer> ps_leftparenthesis = indexesOf(line, '(');
				Vector<Integer> ps_rightparenthesis = indexesOf(line, ')');
				Vector<Integer> ps_comma = indexesOf(line, ',');
				/*
				if (ps_leftparenthesis.isEmpty()) {
					System.err.println("The line in file is wrong, pls check!");
					return false;
				}
				
				if (ps_comma.isEmpty()) {
					System.out.println("There is no none-key attributes");
				}*/
				//get table name
				String tabname = line.substring(0, ps_leftparenthesis.get(0));
				System.out.println(tabname);
				tableNames.add(tabname);
				
				//get the substring
				String sublineStr = line.substring(ps_leftparenthesis.get(0)+1, ps_rightparenthesis.get(ps_rightparenthesis.size()-1));
				
				sublineStr = ',' + sublineStr + ',';
				System.out.println(sublineStr);
				ps_comma = indexesOf(sublineStr, ',');
				Vector<Integer> ps_k = indexesOf(sublineStr, 'k');
				//get candidate key
				ArrayList<String> ck = getCandidateKeyfromLine(sublineStr, ps_comma, ps_k);
				
				//get non-attribute key
				ArrayList<String> nk = getNoneKeyAttributes(sublineStr, ps_comma);
				
				// add candidate key and non-attribute key to lists
				candidateKeys.add(ck);
				nonKeyAttributes.add(nk);
				
				// read second line
				line = bReader.readLine();
			}
			
			bReader.close();
		} catch (IOException e) {
			System.err.println("Cannot read file, pls check path!");
			e.printStackTrace();
			return false;
		}
		
		return true;
	}
	
	//get non-attribute key
	private static ArrayList<String> getNoneKeyAttributes(String line, Vector<Integer> ps_comma) {
		ArrayList<String> nk = new ArrayList<String>();
		/*
		// find commas position that bigger than left-parenthesis
		int lastps_leftparenthesis = ps_leftparenthesis.lastElement();
		for (int i = 0; i < ps_comma.size(); i++) {
			int psc = ps_comma.get(i);
			if (psc > lastps_leftparenthesis) {
				if (i == ps_comma.size() - 1) {
					// last one
					String nkname = line.substring(psc + 1, line.length()-1);
					nk.add(nkname);
				}else{
					// not the last one
					String nkname = line.substring(psc + 1, ps_comma.get(i+1));
					nk.add(nkname);
				}
			}
		}*/
		for (int i = 1; i < ps_comma.size(); i++) {
			String subStr = line.substring(ps_comma.get(i)-1, ps_comma.get(i));
			if (!subStr.equals(")")) {
				int max_comma = 0;
				for (int j = 0; j < ps_comma.size(); j++) {
					if (ps_comma.get(j) < ps_comma.get(i)) {
						if (ps_comma.get(j)>max_comma) {
							max_comma = ps_comma.get(j);
						}
					}
				}
				String ckString = line.substring(max_comma + 1, ps_comma.get(i));
				nk.add(ckString);
			}
		}
		System.out.println("Non-key Attributes: " + nk);
		return nk;
	}

	//get indexes of specific character in string
	public static Vector<Integer> indexesOf(String string, char c) {
		Vector<Integer> ps = new Vector<Integer>();
		if (0 == string.length()) {
			return null;
		}
		
		for(int i = 0; i < string.length(); i++){
			if (string.charAt(i) == c) {
				ps.add(i);
			}
		}
			
		return ps;
	}
	
	//get candidate key
	private static ArrayList<String> getCandidateKeyfromLine(String line, 
			Vector<Integer> ps_comma, Vector<Integer> ps_k) {
		ArrayList<String> ck = new ArrayList<String>();
		/*for (int i = 1; i < ps_leftparenthesis.size(); i++) {
			// double check
			String ckflag = line.substring(ps_leftparenthesis.get(i), ps_leftparenthesis.get(i)+3);
			if (ckflag.equals("(k)")) {
				if (i==1) {
					String ckname = line.substring(ps_leftparenthesis.get(0)+1, ps_leftparenthesis.get(i));
					ck.add(ckname);
				}else {
					String ckname = line.substring(ps_leftparenthesis.get(i-1) + 4, ps_leftparenthesis.get(i));
					ck.add(ckname);
				}
			}else{
				System.err.println("There is a typo in schema!");
				return null;
			}
			
		}*/
		int strLen = line.length();
		for (int i = 0; i < ps_k.size(); i++) {
			if (ps_k.get(i)<1 || ps_k.get(i) > strLen-2) {
				System.err.println("Wrong in reading candidate keys");
			}else{
				String subStr = line.substring(ps_k.get(i) - 1, ps_k.get(i) + 2);
				//System.out.println(subStr);
				if (subStr.equals("(k)")) {
					// search the closest comma position
					int max_comma = 0;
					for (int j = 0; j < ps_comma.size(); j++) {
						if (ps_comma.get(j) < ps_k.get(i)) {
							if (ps_comma.get(j)>max_comma) {
								max_comma = ps_comma.get(j);
							}
						}
					}
					String ckString = line.substring(max_comma + 1, ps_k.get(i)-1);
					ck.add(ckString);
				}
			}
		}
		System.out.println("Candidate key: " + ck);
		return ck;
	}
}
