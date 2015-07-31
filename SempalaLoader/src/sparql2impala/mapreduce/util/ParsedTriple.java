package sparql2impala.mapreduce.util;

/**
 * This class represents the parsed Triple including 
 * the ID() of the object.
 * @author neua
 *
 */
public class ParsedTriple {

	public void setId(String id){
		this.id = id;
	}

	public void setSubject(String subject) {
		this.subject = subject;
	}

	public void setPredicate(String predicate) {
		this.predicate = predicate;
	}

	public void setObject(String object) {
		this.object = object;
	}
	
	private boolean objectTypedLiteral = false;
	
	
	private String objectLiteralType = "";
	
	
	
	public String getObjectLiteralType() {
		return objectLiteralType;
	}

	public boolean isObjectTypedLiteral() {
		return objectTypedLiteral;
	}

	public void setObjectTypedLiteral(boolean objectTypedLiteral) {
		this.objectTypedLiteral = objectTypedLiteral;
	}
	
	public void setObjectLiteralType(String type){
		this.objectLiteralType = type;
	}

	public ParsedTriple(){
		
	}

	public ParsedTriple(String subject, String predicate, String object, String id) {
		super();
		this.subject = subject;
		this.predicate = predicate;
		this.object = object;
		this.id = id;
	}

	private String subject = "";
	private String predicate = "";
	private String object = "";
	private String id = "";

	public String getSubject() {
		return subject;
	}

	public String getPredicate() {
		return predicate;
	}

	public String getObject() {
		return object;
	}

	public String getId() {
		return id;
	}
	
	@Override
	public String toString(){
		return subject+" "+predicate+" "+object+".";
	}


}
