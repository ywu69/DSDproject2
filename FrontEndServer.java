import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class FrontEndServer implements Runnable {

	private ServerSocket serverSocket;
	private ExecutorService threadPool;
	private int serverPort;
	private String ds_addr;
	private int ds_port;
	private String currentPrimary_ipAddrs;
	private int currentPrimary_portNum;
	private static final Logger logger = LogManager.getLogger("FrontEndServer");

	private TweetsMap fe_tweetsMap;

	public FrontEndServer() {
		serverSocket = null;
		threadPool = Executors.newFixedThreadPool(10);
		fe_tweetsMap = new TweetsMap();
	}

	public void run() {
		logger.info("FrontEndServer Start #############################");
		createServerSocket();
		while (true) {
			Socket clientSocket = null;
			try {
				clientSocket = this.serverSocket.accept();
				logger.info("A new request received by FrontEndServer");
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			}
			this.threadPool.execute(new FrontEndProcessor(clientSocket));
		}
	}

	private void createServerSocket() {
		try {
			this.serverSocket = new ServerSocket(this.serverPort);
		} catch (IOException e) {
			logger.debug(e.getMessage(), e);
		}
	}

	private void retrievePrimary() {
		logger.info("Retrieving primary information from discovery server!");

		try {
			Socket registerSocket = new Socket(ds_addr, ds_port);

			BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(
					registerSocket.getOutputStream()));

			String path = "/retrievePrimary";
			wr.write("GET " + path + " HTTP/1.1\r\n");
			wr.write("\r\n");
			wr.flush();

			// Receive primary information from discovery server
			BufferedReader recerive_br = new BufferedReader(
					new InputStreamReader(registerSocket.getInputStream()));

			while (!(recerive_br.readLine().trim()).equals("")) {
			}

			char[] bodyChars = new char[1000];
			recerive_br.read(bodyChars);
			StringBuffer sb = new StringBuffer();
			sb.append(bodyChars);
			String receive_body = sb.toString().trim();

			JSONParser jp = new JSONParser();
			try {
				JSONObject currentPrimary = (JSONObject) jp.parse(receive_body);
				currentPrimary_ipAddrs = currentPrimary.get("ipAddrs")
						.toString();
				currentPrimary_portNum = Integer.valueOf(currentPrimary.get(
						"portNum").toString());
				;
			} catch (ParseException e) {
				logger.debug(e.getMessage(), e);
			}
			logger.info("I'm new frontend, the current primary is: "
					+ currentPrimary_ipAddrs + "  " + currentPrimary_portNum);
			registerSocket.close();
		} catch (IOException e) {
			logger.debug(e.getMessage(), e);
		}
	}

	public static void main(String[] args) {
		FrontEndServer fs = new FrontEndServer();
		fs.serverPort = Integer.valueOf(args[0]);
		try {
			fs.ds_addr = InetAddress.getByName(args[1]).getHostAddress();
		} catch (UnknownHostException e) {
			logger.debug(e.getMessage(), e);
		}
		fs.ds_port = Integer.valueOf(args[2]);
		fs.retrievePrimary();
		new Thread(fs).start();
	}

	/*
	 * FrontEndProcessor Thread
	 */
	class FrontEndProcessor implements Runnable {

		private Socket clientSocket = null;

		public FrontEndProcessor(Socket clientSocket) {
			this.clientSocket = clientSocket;
		}

		public void run() {
			try {
				/*
				 * Read request from client and parse it by
				 * HTTPRequestLineParser
				 */
				BufferedReader br = new BufferedReader(new InputStreamReader(
						clientSocket.getInputStream()));
				String line;
				ArrayList<String> postHearder = new ArrayList<String>();

				while (!(line = br.readLine().trim()).equals("")) {
					postHearder.add(line);
				}
				String uri = null;
				uri = postHearder.get(0);

				OutputStream out = clientSocket.getOutputStream();
				HTTPRequestLine hl = HTTPRequestLineParser.parse(uri);
				HTTPResponseHandler hrh = new HTTPResponseHandler(out);

				/*
				 * Check whether the request is valid
				 */
				if (hl != null) {
					/*
					 * 404 Not found returned for bad endpoint
					 */
					if (!hl.getUripath().equals("tweets")) {
						hrh.response(404, "Not Found", "Not Found!");
						logger.info("This request has a bad endpoint");
					} else {
						/*
						 * If this is a post requst, call postProcessor, If this
						 * is a get request, call searchProcessor, Otherwise, we
						 * do not provide this method
						 */
						String httpMethod = hl.getMethod().toString();
						if (httpMethod.equals("POST")) {
							logger.info("Post a tweet to data server.................");
							postProcessor(br, hrh);
						} else if (httpMethod.equals("GET")) {
							logger.info("Search a serchterm in data server.................");
							searchProcessor(hl, hrh);
						} else {
							hrh.response(405, "Method Not Allowed",
									"We do not provide this method!");
							logger.info("We do not provide this method");
						}
					}
				} else {
					hrh.response(400, "Bad Request", "This is a bad request!");
					logger.info("This is a bad request!");
				}
				out.flush();
				out.close();
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			}
		}

		@SuppressWarnings("unchecked")
		private void postProcessor(BufferedReader br, HTTPResponseHandler hrh) {
			String valueofRequestBody = null;
			JSONObject addTweetBody = new JSONObject();
			try {
				/*
				 * If the post request body is not empty, read it in, Otherwise,
				 * response a 400 Bad Request
				 */
				if (br.ready()) {
					/*
					 * Read the request body in
					 */
					char[] bodyChars = new char[1000];
					br.read(bodyChars);
					StringBuffer sb = new StringBuffer();
					sb.append(bodyChars);
					String postBody = sb.toString().trim();

					JSONParser jp = new JSONParser();
					JSONObject requestBody = (JSONObject) jp.parse(postBody);
					/*
					 * If the post request body contains "text", parse the tweet
					 * in Otherwise, response a 400 Bad Request
					 */
					if (requestBody.containsKey("text")) {
						valueofRequestBody = requestBody.get("text").toString();
						/*
						 * If the tweet contains no hashtag, response a 400 Bad
						 * Request
						 */
						if (!valueofRequestBody.contains("#")) {
							hrh.response(400, "Bad Request",
									"Your tweet does not have any hashtag!");
							logger.info("No hashtag in this tweet!");
						} else {
							String[] tweetStrings = null;
							JSONArray hashtags = new JSONArray();
							tweetStrings = valueofRequestBody.split(" ");
							for (int i = 0; i < tweetStrings.length; i++) {
								if (tweetStrings[i].contains("#")) {
									/*
									 * If the tweet contains empty hashtag,
									 * response a 400 Bad Request, Otherwise,
									 * get every hashtag and tweet, Then post it
									 * to Data Server
									 */
									if (tweetStrings[i].substring(1).isEmpty()) {
										hrh.response(400, "Bad Request",
												"At least one hashtag is empty!");
										logger.info("At least one hashtag is empty!");
										return;
									} else {
										hashtags.add(tweetStrings[i]
												.substring(1));
									}
								}
							}

							addTweetBody.put("tweet", valueofRequestBody);
							addTweetBody.put("hashtags", hashtags);

							/*
							 * Send post message to data server
							 */
							Socket postSocket = new Socket(
									currentPrimary_ipAddrs,
									currentPrimary_portNum);
							BufferedWriter wr = new BufferedWriter(
									new OutputStreamWriter(
											postSocket.getOutputStream()));

							String path = "/tweets";
							wr.write("POST " + path + " HTTP/1.1\r\n");
							wr.write("Content-Length: "
									+ valueofRequestBody.length() + "\r\n");
							wr.write("Content-Type: application/json\r\n");
							wr.write("\r\n");
							wr.write(addTweetBody.toString());
							wr.flush();

							/*
							 * Respond to client
							 */
							BufferedReader recerive_br = new BufferedReader(
									new InputStreamReader(
											postSocket.getInputStream()));

							String receive_line = null;
							ArrayList<String> receive_Hearder = new ArrayList<String>();

							String line = recerive_br.readLine();
							if (line == null) {
								/*
								 * Retrieve primary again and send post message
								 * to data server again
								 */
								try {
									Thread.sleep(5000);
								} catch (InterruptedException e2) {
									logger.debug(e2.getMessage(), e2);
								}
								retrievePrimary();

								try {
									Socket postSocket_2 = new Socket(
											currentPrimary_ipAddrs,
											currentPrimary_portNum);
									BufferedWriter wr_2 = new BufferedWriter(
											new OutputStreamWriter(
													postSocket_2
															.getOutputStream()));

									String path_2 = "/tweets";
									wr_2.write("POST " + path_2
											+ " HTTP/1.1\r\n");
									wr_2.write("Content-Length: "
											+ valueofRequestBody.length()
											+ "\r\n");
									wr_2.write("Content-Type: application/json\r\n");
									wr_2.write("\r\n");
									wr_2.write(addTweetBody.toString());
									wr_2.flush();

									/*
									 * Respond to client
									 */
									BufferedReader recerive_br_2 = new BufferedReader(
											new InputStreamReader(
													postSocket_2
															.getInputStream()));
									String receive_line_2 = null;
									ArrayList<String> receive_Hearder_2 = new ArrayList<String>();

									while (!(receive_line_2 = recerive_br_2
											.readLine().trim()).equals("")) {
										receive_Hearder_2.add(receive_line_2);
									}

									String[] responseHeader_2 = null;
									responseHeader_2 = receive_Hearder_2.get(0)
											.trim().split(" ");

									String responseType_2 = null;
									responseType_2 = responseHeader_2[1];

									if (responseType_2.equals("201")) {
										logger.info("Post a tweet into data server successfully!");
										hrh.response(201, "Created", "Created!");
									} else if (responseType_2.equals("406")) {
										logger.info("Post a tweet into data server successfully!");
										hrh.response(201, "Created", "Created!");
									}

									postSocket_2.close();
								} catch (IOException e1) {
									logger.debug(e1.getMessage(), e1);
									logger.info("Primary Server Unavailable!");
									hrh.response(503, "Service Unavailable",
											"Primary Server Unavailable, please try later!");
								}
							} else {
								receive_Hearder.add(line);
								while (!(receive_line = recerive_br.readLine()
										.trim()).equals("")) {
									receive_Hearder.add(receive_line);
								}
								String[] responseHeader = null;
								responseHeader = receive_Hearder.get(0).trim()
										.split(" ");

								String responseType = null;
								responseType = responseHeader[1];

								if (responseType.equals("201")) {
									logger.info("Post a tweet into data server successfully!");
									hrh.response(201, "Created", "Created!");
								} else if (responseType.equals("406")) {
									hrh.response(406, "Not Acceptable",
											"This tweet exists in data server!");
									logger.info("This tweet exists in data server!");
								}
							}

							postSocket.close();
						}
					} else {
						hrh.response(400, "Bad Request",
								"You do not post any tweet!");
						logger.info("No tweet in this post request!");
					}
				} else {
					hrh.response(400, "Bad Request",
							"You do not post anything!");
					logger.info("Post request body is empty!");
				}
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
				
				retrievePrimary();
				try {
					Socket postSocket_2 = new Socket(currentPrimary_ipAddrs,
							currentPrimary_portNum);
					BufferedWriter wr_2 = new BufferedWriter(
							new OutputStreamWriter(
									postSocket_2.getOutputStream()));

					String path_2 = "/tweets";
					wr_2.write("POST " + path_2 + " HTTP/1.1\r\n");
					wr_2.write("Content-Length: " + valueofRequestBody.length()
							+ "\r\n");
					wr_2.write("Content-Type: application/json\r\n");
					wr_2.write("\r\n");
					wr_2.write(addTweetBody.toString());
					wr_2.flush();

					/*
					 * Respond to client
					 */
					BufferedReader recerive_br_2 = new BufferedReader(
							new InputStreamReader(postSocket_2.getInputStream()));
					String receive_line_2 = null;
					ArrayList<String> receive_Hearder_2 = new ArrayList<String>();

					while (!(receive_line_2 = recerive_br_2.readLine().trim())
							.equals("")) {
						receive_Hearder_2.add(receive_line_2);
					}

					String[] responseHeader_2 = null;
					responseHeader_2 = receive_Hearder_2.get(0).trim()
							.split(" ");

					String responseType_2 = null;
					responseType_2 = responseHeader_2[1];

					if (responseType_2.equals("201")) {
						logger.info("Post a tweet into data server successfully!");
						hrh.response(201, "Created", "Created!");
					} else if (responseType_2.equals("406")) {
						logger.info("Post a tweet into data server successfully!");
						hrh.response(201, "Created", "Created!");
					}

					postSocket_2.close();
				} catch (IOException e2) {
					logger.debug(e2.getMessage(), e2);
					logger.info("Primary Server Unavailable!");
					hrh.response(503, "Service Unavailable",
							"Primary Server Unavailable, please try later!");
				}
			} catch (ParseException e) {
				logger.debug(e.getMessage(), e);
			}
		}

		@SuppressWarnings("unchecked")
		private void searchProcessor(HTTPRequestLine hl, HTTPResponseHandler hrh) {
			HashMap<String, String> searchtermHashMap = hl.getParameters();

			/*
			 * If the parameter map is empty, that means the request contains no
			 * q
			 */
			if (searchtermHashMap.isEmpty()) {
				hrh.response(400, "Bad Request",
						"This GET request does not have q parameter!");
				logger.info("No parameter in this GET request!");
			} else if (!searchtermHashMap.containsKey("q")) {
				/*
				 * If the request contains other parameters, response a 400 Bad
				 * Request
				 */
				hrh.response(400, "Bad Request",
						"We only accept q as parameter!");
				logger.info("We only accept q parameter in the GET request!");
			} else {

				try {
					/*
					 * Send search message to data server
					 */
					Socket connectToDsSocket = new Socket(
							currentPrimary_ipAddrs, currentPrimary_portNum);
					BufferedWriter wr = new BufferedWriter(
							new OutputStreamWriter(
									connectToDsSocket.getOutputStream()));

					String searchterm = searchtermHashMap.get("q");
					int cache_versionNum = -1;

					/*
					 * If the cache does not have this searchterm, set the
					 * versionNum to -1 Then send the GET request to data server
					 */
					logger.info("Sending a search request to data server...............");

					if (!fe_tweetsMap.containsKey(searchterm)) {
						wr.write("GET " + "/tweets?q=" + searchterm + "&"
								+ "v=" + "-1" + " HTTP/1.1\r\n");
						wr.write("\r\n");
					}
					/*
					 * If the cache has this searchterm, set the versionNum to
					 * cache_versionNum Then send the GET request to data server
					 */
					else {
						cache_versionNum = fe_tweetsMap
								.getVersionNum(searchterm);

						wr.write("GET " + "/tweets?q=" + searchterm + "&"
								+ "v=" + cache_versionNum + " HTTP/1.1\r\n");
						wr.write("\r\n");
					}
					wr.flush();

					/*
					 * Receive the response from data server
					 */
					logger.info("Receiving a search response from data server...............");

					BufferedReader recerive_br = new BufferedReader(
							new InputStreamReader(
									connectToDsSocket.getInputStream()));
					String receive_line = null;
					ArrayList<String> receive_Hearder = new ArrayList<String>();

					String line = recerive_br.readLine();
					if (line == null) {
						logger.info("Primary Server Unavailable!");
						hrh.response(503, "Service Unavailable",
								"Primary Server Unavailable, please try later!");
					} else {
						receive_Hearder.add(line);
						while (!(receive_line = recerive_br.readLine().trim())
								.equals("")) {
							receive_Hearder.add(receive_line);
						}

						char[] bodyChars = new char[1000];
						recerive_br.read(bodyChars);
						StringBuffer sb = new StringBuffer();
						sb.append(bodyChars);
						String receive_body = sb.toString().trim();

						/*
						 * If the response from data server is
						 * "Your version is up-to-date!", get the tweets of this
						 * serachterm from cache and send it to client
						 */
						if (receive_body.equals("Your version is up-to-date!")) {
							JSONArray final_tweetArray = fe_tweetsMap
									.getTweetsArray(searchterm);
							JSONObject final_response = new JSONObject();
							final_response.put("q", searchterm);
							final_response.put("tweets", final_tweetArray);
							hrh.response(200, "OK", final_response.toString());
							logger.info("Response to client directly............");
						}
						/*
						 * If the response from data server is not
						 * "Your version is up-to-date!", get the tweets of this
						 * serachterm from data server and send it to client
						 */
						else {
							JSONParser jp = new JSONParser();
							JSONObject receiveBody = (JSONObject) jp
									.parse(receive_body);
							int ds_versionNum = Integer.valueOf(receiveBody
									.get("v").toString());

							JSONObject final_response = new JSONObject();
							JSONArray final_tweetArray = (JSONArray) receiveBody
									.get("tweets");

							/*
							 * If the versionNum in the response is -1, that
							 * means the data server also contains no this
							 * searchterm Then send an response with empty tweet
							 * array to client
							 */
							if (ds_versionNum == -1) {
								final_response.put("q", searchterm);
								final_response.put("tweets", final_tweetArray);
								hrh.response(200, "OK",
										final_response.toString());
								logger.info("No search result............");
							}
							/*
							 * If the versionNum in the response is not -1, that
							 * means the cache need to update its version, Then
							 * send an response with the tweets about this
							 * serchterm from data server to client, Then update
							 * the cache to latest version
							 */
							else {
								final_response.put("q", searchterm);
								final_response.put("tweets", final_tweetArray);
								hrh.response(200, "OK",
										final_response.toString());
								logger.info("Response to client and update the cache............");
								/* Update CacheHashMap */
								TweetsData final_tweetsData = new TweetsData();
								final_tweetsData.setVersionNum(ds_versionNum);
								final_tweetsData
										.setTweetsArray(final_tweetArray);
								fe_tweetsMap.setTweetsHashMap(searchterm,
										final_tweetsData);
							}
						}
					}

					connectToDsSocket.close();
				} catch (IOException e) {
					logger.debug(e.getMessage(), e);
					try {
						/*
						 * Retrieve primary again and send search message to
						 * data server again
						 */
						try {
							Thread.sleep(5000);
						} catch (InterruptedException e2) {
							logger.debug(e2.getMessage(), e2);
						}
						retrievePrimary();
						Socket connectToDsSocket = new Socket(
								currentPrimary_ipAddrs, currentPrimary_portNum);
						BufferedWriter wr = new BufferedWriter(
								new OutputStreamWriter(
										connectToDsSocket.getOutputStream()));

						String searchterm = searchtermHashMap.get("q");
						int cache_versionNum = -1;

						/*
						 * If the cache does not have this searchterm, set the
						 * versionNum to -1 Then send the GET request to data
						 * server
						 */
						logger.info("Sending a search request to data server...............");

						if (!fe_tweetsMap.containsKey(searchterm)) {
							wr.write("GET " + "/tweets?q=" + searchterm + "&"
									+ "v=" + "-1" + " HTTP/1.1\r\n");
							wr.write("\r\n");
						}
						/*
						 * If the cache has this searchterm, set the versionNum
						 * to cache_versionNum Then send the GET request to data
						 * server
						 */
						else {
							cache_versionNum = fe_tweetsMap
									.getVersionNum(searchterm);

							wr.write("GET " + "/tweets?q=" + searchterm + "&"
									+ "v=" + cache_versionNum + " HTTP/1.1\r\n");
							wr.write("\r\n");
						}
						wr.flush();

						/*
						 * Receive the response from data server
						 */
						logger.info("Receiving a search response from data server...............");

						BufferedReader recerive_br = new BufferedReader(
								new InputStreamReader(
										connectToDsSocket.getInputStream()));
						String receive_line = null;
						ArrayList<String> receive_Hearder = new ArrayList<String>();

						while (!(receive_line = recerive_br.readLine().trim())
								.equals("")) {
							receive_Hearder.add(receive_line);
						}

						char[] bodyChars = new char[1000];
						recerive_br.read(bodyChars);
						StringBuffer sb = new StringBuffer();
						sb.append(bodyChars);
						String receive_body = sb.toString().trim();

						/*
						 * If the response from data server is
						 * "Your version is up-to-date!", get the tweets of this
						 * serachterm from cache and send it to client
						 */
						if (receive_body.equals("Your version is up-to-date!")) {
							JSONArray final_tweetArray = fe_tweetsMap
									.getTweetsArray(searchterm);
							JSONObject final_response = new JSONObject();
							final_response.put("q", searchterm);
							final_response.put("tweets", final_tweetArray);
							hrh.response(200, "OK", final_response.toString());
							logger.info("Response to client directly............");
						}
						/*
						 * If the response from data server is not
						 * "Your version is up-to-date!", get the tweets of this
						 * serachterm from data server and send it to client
						 */
						else {
							JSONParser jp = new JSONParser();
							JSONObject receiveBody = (JSONObject) jp
									.parse(receive_body);
							int ds_versionNum = Integer.valueOf(receiveBody
									.get("v").toString());

							JSONObject final_response = new JSONObject();
							JSONArray final_tweetArray = (JSONArray) receiveBody
									.get("tweets");

							/*
							 * If the versionNum in the response is -1, that
							 * means the data server also contains no this
							 * searchterm Then send an response with empty tweet
							 * array to client
							 */
							if (ds_versionNum == -1) {
								final_response.put("q", searchterm);
								final_response.put("tweets", final_tweetArray);
								hrh.response(200, "OK",
										final_response.toString());
								logger.info("No search result............");
							}
							/*
							 * If the versionNum in the response is not -1, that
							 * means the cache need to update its version, Then
							 * send an response with the tweets about this
							 * serchterm from data server to client, Then update
							 * the cache to latest version
							 */
							else {
								final_response.put("q", searchterm);
								final_response.put("tweets", final_tweetArray);
								hrh.response(200, "OK",
										final_response.toString());
								logger.info("Response to client and update the cache............");
								/* Update CacheHashMap */
								TweetsData final_tweetsData = new TweetsData();
								final_tweetsData.setVersionNum(ds_versionNum);
								final_tweetsData
										.setTweetsArray(final_tweetArray);
								fe_tweetsMap.setTweetsHashMap(searchterm,
										final_tweetsData);
							}
						}
						connectToDsSocket.close();
					} catch (IOException e1) {
						logger.debug(e1.getMessage(), e1);
						logger.info("Primary Server Unavailable!");
						hrh.response(503, "Service Unavailable",
								"Primary Server Unavailable, please try later!");
					} catch (ParseException e1) {
						logger.debug(e1.getMessage(), e1);
					}
				} catch (ParseException e) {
					logger.debug(e.getMessage(), e);
				}
			}
		}
	}
}