/**
HTTPConstants provides a set of constant values used for convenience.
 **/
public class HTTPConstants {

	public enum HTTPMethod {
		OPTIONS, GET, HEAD, POST, PUT, DELETE, TRACE, CONNECT;
		
		public String toString(){
	        switch(this){
	        case OPTIONS :
	            return "OPTIONS";
	        case GET:
	            return "GET";
	        case HEAD:
	            return "HEAD";
	        case POST:
	            return "POST";
	        case PUT:
	            return "PUT";
	        case DELETE:
	            return "DELETE";
	        case TRACE:
	            return "TRACE";
	        case CONNECT:
	            return "CONNECT";
	        }
	        return null;
	    }
	};	
}