import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

@SuppressWarnings("unchecked")
public class DiscoveryServer implements Runnable {

	private ServerSocket serverSocket;
	private ExecutorService threadPool;
	private int serverPort;

	private DiscoveryData discoveryData;

	private static final Logger logger = LogManager
			.getLogger("DiscoveryServer");

	public DiscoveryServer(int port) {
		serverSocket = null;
		threadPool = Executors.newFixedThreadPool(10);
		discoveryData = new DiscoveryData();
		this.serverPort = port;
	}

	public void run() {
		// Start monitoring
		createServerSocket();
		while (true) {
			Socket clientSocket = null;
			try {
				clientSocket = this.serverSocket.accept();
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			}
			this.threadPool.execute(new DiscoveryProcessor(clientSocket));
		}
	}

	private void createServerSocket() {
		try {
			this.serverSocket = new ServerSocket(this.serverPort);
		} catch (IOException e) {
			logger.debug(e.getMessage(), e);
		}
	}

	public static void main(String[] args) {
		logger.info("DiscoveryServer Start #############################");
		int PORT = 4005;
		DiscoveryServer ds = new DiscoveryServer(PORT);
		new Thread(ds).start();
		// Detect primary alive every 5 second
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(ds.new DetectThread(), 0, 1000);
	}

	class DetectThread extends TimerTask {
		public void run() {
			sendDetection();
		}

		private void sendDetection() {

			JSONArray currentBackEndsArray = discoveryData.getBackEndsArray();

			if (currentBackEndsArray.size() == 0) {
				logger.info("No data server running now!");
			} else {
				int currentPrimaryID = Integer.valueOf(discoveryData
						.getCurrentPrimary().get("dataserverID").toString());
				for (int i = 0; i < currentBackEndsArray.size(); i++) {
					JSONObject eachBackEnd = (JSONObject) currentBackEndsArray
							.get(i);
					int dateserverID = Integer.valueOf(eachBackEnd.get(
							"dataserverID").toString());
					String eachBackEnd_ipAddrs = eachBackEnd.get("ipAddrs")
							.toString();
					int eachBackEnd_portNum = Integer.valueOf(eachBackEnd.get(
							"portNum").toString());
					try {
						Socket detectSocket = new Socket(eachBackEnd_ipAddrs,
								eachBackEnd_portNum);

						BufferedWriter wr = new BufferedWriter(
								new OutputStreamWriter(
										detectSocket.getOutputStream()));

						String path = "/detection";
						wr.write("GET " + path + " HTTP/1.1\r\n");
						wr.write("\r\n");
						wr.flush();

						wr.close();

//						BufferedReader recerive_br = new BufferedReader(
//								new InputStreamReader(
//										detectSocket.getInputStream()));
//
//						while (!(recerive_br.readLine().trim()).equals("")) {
//						}
//
//						if (recerive_br.ready()) {
//							char[] bodyChars = new char[1000];
//							recerive_br.read(bodyChars);
//							StringBuffer sb = new StringBuffer();
//							sb.append(bodyChars);
//							String receive_body = sb.toString().trim();
//
//							if (receive_body.equals("I'm alive!")) {
//								if (dateserverID == currentPrimaryID) {
//									logger.info("Primary dataserver is alive!");
//								} else {
//									logger.info("No." + dateserverID
//											+ " secondary dataserver is alive!");
//								}
//							}
//						}

						detectSocket.close();
					} catch (IOException e) {
						logger.debug(e.getMessage(), e);
						if (dateserverID == currentPrimaryID) {
							logger.info("Primary dataserver is down, waiting for new primary!");
						} else {
							logger.info("No."
									+ dateserverID
									+ " secondary dataserver is down, remove it from backEnds array!");
						}
						discoveryData.removeBackEnd(eachBackEnd);
					}
				}
			}
		}
	}

	class DiscoveryProcessor implements Runnable {

		private Socket clientSocket = null;

		public DiscoveryProcessor(Socket clientSocket) {
			this.clientSocket = clientSocket;
		}

		public void run() {

			try {
				/*
				 * Read request and parse it by HTTPRequestLineParser
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
					if (hl.getUripath().equals("register")) {
						String httpMethod = hl.getMethod().toString();
						if (httpMethod.equals("POST")) {
							logger.info("Receive a register request from a new back end.................");
							registerAsBackend(br, hrh);
						} else {
							hrh.response(405, "Method Not Allowed",
									"We do not provide this method!");
							logger.info("We do not provide this method");
						}
					} else if (hl.getUripath().equals("retrievePrimary")) {
						String httpMethod = hl.getMethod().toString();
						if (httpMethod.equals("GET")) {
							logger.info("Receive a retrievePrimary request from a front end.................");
							responsePrimaryInfo(hrh);
						} else {
							hrh.response(405, "Method Not Allowed",
									"We do not provide this method!");
							logger.info("We do not provide this method");
						}
					} else if (hl.getUripath().equals("notifyNewPrimary")) {
						String httpMethod = hl.getMethod().toString();
						if (httpMethod.equals("POST")) {
							logger.info("Receive new primary notify ##################");
							updateCurrentPrimary(br);
						} else {
							hrh.response(405, "Method Not Allowed",
									"We do not provide this method!");
							logger.info("We do not provide this method");
						}
					} else {
						hrh.response(404, "Not Found", "Not Found!");
						logger.info("This request has a bad endpoint");
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

		private void registerAsBackend(BufferedReader br,
				HTTPResponseHandler hrh) {
			try {
				/*
				 * If the post request contains body, read it in, Otherwise,
				 * return a 400 Bad Request
				 */
				if (br.ready()) {
					char[] bodyChars = new char[1000];
					br.read(bodyChars);
					StringBuffer sb = new StringBuffer();
					sb.append(bodyChars);
					String postBody = sb.toString().trim();

					JSONParser jp = new JSONParser();
					JSONObject requestBody = (JSONObject) jp.parse(postBody);

					String ipAddrs = requestBody.get("ipAddrs").toString();
					int portNum = Integer.valueOf(requestBody.get("portNum")
							.toString());
					int dataserverID = discoveryData.getDataserverID();
					
					JSONObject newBackEndObject = new JSONObject();
					newBackEndObject.put("dataserverID", dataserverID);
					newBackEndObject.put("ipAddrs", ipAddrs);
					newBackEndObject.put("portNum", portNum);

					JSONArray currentBackEndsArray = discoveryData
							.getBackEndsArray();

					/*
					 * Notify all data servers about the new backend
					 */
					for (int i = 0; i < currentBackEndsArray.size(); i++) {
						JSONObject eachBackEnd = (JSONObject) currentBackEndsArray
								.get(i);
						String eachBackEnd_ipAddrs = eachBackEnd.get("ipAddrs")
								.toString();
						int eachBackEnd_portNum = Integer.valueOf(eachBackEnd
								.get("portNum").toString());
						try {
							Socket notifySocket = new Socket(
									eachBackEnd_ipAddrs, eachBackEnd_portNum);

							BufferedWriter wr = new BufferedWriter(
									new OutputStreamWriter(
											notifySocket.getOutputStream()));

							String path = "/notifyNewBackend";
							wr.write("POST " + path + " HTTP/1.1\r\n");
							wr.write("\r\n");
							wr.write(newBackEndObject.toString());
							wr.flush();
							wr.close();
							notifySocket.close();
						} catch (IOException e) {
							logger.debug(e.getMessage(), e);
						}
					}

					/*
					 * Add new backend to backEndsArray
					 */
					discoveryData.addBackEnd(ipAddrs, portNum);

					/*
					 * Response to new backend
					 */
					JSONObject currentPrimary = discoveryData
							.getCurrentPrimary();

					JSONObject responseBody = new JSONObject();
					responseBody.put("dataserverID", dataserverID);
					responseBody.put("currentPrimary", currentPrimary);
					responseBody.put("currentBackEndsArray",
							currentBackEndsArray);

					hrh.response(200, "OK", responseBody.toString());
					logger.info("Register successfully!");
				} else {
					hrh.response(400, "Bad Request",
							"Front End do not post anything!");
					logger.info("Post request body is empty!");
				}
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			} catch (ParseException e) {
				logger.debug(e.getMessage(), e);
			}
		}

		private void responsePrimaryInfo(HTTPResponseHandler hrh) {
			/*
			 * Response to new frontend
			 */
			JSONObject currentPrimary = discoveryData.getCurrentPrimary();
			hrh.response(200, "OK", currentPrimary.toString());
		}

		private void updateCurrentPrimary(BufferedReader br) {
			try {
				char[] bodyChars = new char[1000];
				br.read(bodyChars);
				StringBuffer sb = new StringBuffer();
				sb.append(bodyChars);
				String postBody = sb.toString().trim();

				JSONParser jp = new JSONParser();
				JSONObject newPrimaryObject = (JSONObject) jp.parse(postBody);

				discoveryData.setCurrentPrimary(newPrimaryObject);
				logger.info("########################## Update current primary to the new primary: "
						+ discoveryData.getCurrentPrimary());
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			} catch (ParseException e) {
				logger.debug(e.getMessage(), e);
			}
		}
	}
}