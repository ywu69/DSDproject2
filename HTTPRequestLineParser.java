import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HTTPRequestLineParser {

	/**
	 * This method takes as input the Request-Line exactly as it is read from
	 * the socket. It returns a Java object of type HTTPRequestLine containing a
	 * Java representation of the line.
	 * 
	 * The signature of this method may be modified to throw exceptions you feel
	 * are appropriate. The parameters and return type may not be modified.
	 * 
	 * 
	 * @param line
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public static HTTPRequestLine parse(String line)
			throws UnsupportedEncodingException {
		// /A Request-Line is a METHOD followed by SPACE followed by URI
		// followed by SPACE followed by VERSION
		// A VERSION is 'HTTP/' followed by 1.0 or 1.1
		// A URI is a '/' followed by PATH followed by optional '?' PARAMS
		// PARAMS are of the form key'='value'&'

		HTTPRequestLine httpRequestLine = new HTTPRequestLine();
		String[] lineArray = line.split(" ");

		if (lineArray.length == 3) {

			// METHOD
			try {
				httpRequestLine.setMethod(HTTPConstants.HTTPMethod
						.valueOf(lineArray[0]));
			} catch (IllegalArgumentException e) {
				e.getMessage();
				return null;
			}

			// URI
			String uri = lineArray[1];
			String regx = "/[^?]+([?]?([^?]+=[^?]+&?)+)*";

			Pattern p = Pattern.compile(regx);
			Matcher m = p.matcher(lineArray[1]);
			if (!m.matches()) {
				return null;
			}

			// HTTP VERSION
			String httpVersion = lineArray[2];
			if (!httpVersion.equals("HTTP/1.0")
					&& !httpVersion.equals("HTTP/1.1")) {
				return null;
			}
			httpRequestLine.setHttpversion(httpVersion);

			// URI PATH
			if (uri.contains("?")) {
				ArrayList<String> uriArray = new ArrayList<String>();
				int qm_index = uri.indexOf("?");
				uriArray.add(uri.substring(0, qm_index));
				uriArray.add(uri.substring(qm_index + 1));
				httpRequestLine.setUripath(uriArray.get(0).substring(1));

				// PARAMS
				if (uriArray.size() > 1) {
					String paramString = uriArray.get(1);
					String[] params = paramString.split("&");
					for (String param : params) {
						String[] keyandvalue = param.split("=");
						httpRequestLine.setParameters(
								URLDecoder.decode(keyandvalue[0], "UTF-8"),
								URLDecoder.decode(keyandvalue[1], "UTF-8"));
					}
				}
			} else {
				httpRequestLine.setUripath(uri.substring(1));
			}

			return httpRequestLine;
		} else {
			return null;
		}
	}
}