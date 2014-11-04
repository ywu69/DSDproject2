import java.util.HashMap;

/**
 * HTTPRequestLine is a data structure that stores a Java representation of the
 * parsed Request-Line.
 **/
public class HTTPRequestLine {

	private HTTPConstants.HTTPMethod method = null;
	private String uripath = null;
	private HashMap<String, String> parameters = new HashMap<String, String>();
	private String httpversion = null;

	/*
	 * You are expected to add appropriate constructors/getters/setters to
	 * access and update the data in this class.
	 */
	public HTTPRequestLine() {

	}

	public HTTPConstants.HTTPMethod getMethod() {
		return this.method;
	}

	public String getUripath() {
		return this.uripath;
	}

	public HashMap<String, String> getParameters() {
		return this.parameters;
	}

	public String getHttpversion() {
		return this.httpversion;
	}

	public void setMethod(HTTPConstants.HTTPMethod httpMethod) {
		this.method = httpMethod;
	}

	public void setUripath(String uriPath) {
		this.uripath = uriPath;
	}
	
	public void setParameters(String key, String value) {
		this.parameters.put(key, value);
	}

	public void setHttpversion(String httpVersion) {
		this.httpversion = httpVersion;
	}
	
	public String toString(){
		String method = null;
		if (getMethod()!=null) {
			method = getMethod().toString();
		}
		return "  Method: " + method + "\n" + "  Uripath: " + getUripath() + "\n" + 
				"  Parameters: " + getParameters() + "\n" + "  HttpVersion: " + getHttpversion() + "\n";
	}
}