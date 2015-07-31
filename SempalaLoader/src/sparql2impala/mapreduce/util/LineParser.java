package sparql2impala.mapreduce.util;

import java.text.ParseException;


/**
 * This class parses the lines of the interim results and initial rdf input. If
 * ID not present, ID is set to 0.
 * 
 * @author neua
 * 
 */
public class LineParser {
	// ommit 4th element
	boolean isQuadTriple = false;
	
	/**
	 * Standard constructor for n-triple.
	 */
	public LineParser(){
		this.isQuadTriple = false;
	}
	
	public LineParser(boolean isQuadTriple){
		this.isQuadTriple = isQuadTriple;
	}
	
	
	
	public ParsedTriple parseLine(String line) throws ParseException {
		if(isQuadTriple){
			String idPart ="";
			boolean hasID = false;
			if(line.indexOf("@:!!") != -1){
				hasID = true;
				idPart = line.substring(line.indexOf("@:!!"));
			}
			
			if(hasID){
				line = line.substring(0, line.indexOf("@:!!")).trim();	
			}
			
			line = line.substring(0, line.lastIndexOf(" "));
			line = line.substring(0, line.lastIndexOf(" "));
			line += " . ";
			line += idPart;
		}

		ParsedTriple result = new ParsedTriple();
		int index = line.indexOf("@:!!");
		if(index != -1){
			result.setId(line.substring(index+4).trim());
			line = line.substring(0, index);
		} else{
			result.setId("-1");
		}

		if (line.indexOf("@") == 0) {
			return new ParsedTriple();
		}

		String[] res = ExNTriplesParser.parseTriple(line, "\t", false);
		try {
		result.setSubject(res[0]);
		result.setPredicate(res[1]);
		result.setObject(res[2]);
		if(res.length == 4){
			result.setObjectTypedLiteral(true);
			result.setObjectLiteralType(res[3]);
		} else {
			result.setObjectTypedLiteral(false);
		}
		} catch(NullPointerException e){
			System.out.println("Got null pointer exception on line: "+line);
		} catch (Exception e){
			System.out.println("Got other exception on line: "+line);
		}
		
		
		return result;
	}



}
