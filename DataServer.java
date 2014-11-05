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
import java.util.Map;
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
public class DataServer implements Runnable {

	private ServerSocket serverSocket;
	private ExecutorService threadPool;
	private String serverIpAddrs;
	private int serverPort;
	private String discovery_ipAddrs;
	private int discovery_portNum;

	private TweetsMap ds_tweetsMap;

	private static final Logger logger = LogManager.getLogger("DataServer");

	private int dataserverID;
	private JSONObject currentPrimary;
	private JSONArray backEndsArray;

	private enum DataServerRole {
		PRIMARY, SECONDARY, NONE
	};

	private DataServerRole dataServerRole;

	public DataServer(String self_ipAddrs, int self_port,
			String discovery_ipAddrs, int discovery_portNum) {
		this.serverIpAddrs = self_ipAddrs;
		this.serverPort = self_port;
		this.discovery_ipAddrs = discovery_ipAddrs;
		this.discovery_portNum = discovery_portNum;

		serverSocket = null;
		threadPool = Executors.newFixedThreadPool(10);

		ds_tweetsMap = new TweetsMap();
		dataserverID = Integer.MAX_VALUE;
		backEndsArray = new JSONArray();
		currentPrimary = new JSONObject();
		dataServerRole = DataServerRole.NONE;
	}

	public void run() {
		logger.info("DataServer Start #############################");

		// Start monitoring
		createServerSocket();
		while (true) {
			Socket clientSocket = null;
			try {
				clientSocket = this.serverSocket.accept();
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			}
			this.threadPool.execute(new DataProcessor(clientSocket));
		}
	}

	private void createServerSocket() {
		try {
			this.serverSocket = new ServerSocket(this.serverPort);
		} catch (IOException e) {
			logger.debug(e.getMessage(), e);
		}
	}

	private void registerForDiscovery() {
		logger.info("Registering for discovery server!");

		JSONObject registerInfo = new JSONObject();
		registerInfo.put("ipAddrs", serverIpAddrs);
		registerInfo.put("portNum", serverPort);
		try {
			Socket registerSocket = new Socket(discovery_ipAddrs,
					discovery_portNum);

			BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(
					registerSocket.getOutputStream()));

			String path = "/register";
			wr.write("POST " + path + " HTTP/1.1\r\n");
			wr.write("Content-Length: " + registerInfo.toString().length()
					+ "\r\n");
			wr.write("Content-Type: application/json\r\n");
			wr.write("\r\n");
			wr.write(registerInfo.toString());
			wr.flush();

			BufferedReader recerive_br = new BufferedReader(
					new InputStreamReader(registerSocket.getInputStream()));

			String receive_line = null;
			ArrayList<String> receive_Hearder = new ArrayList<String>();

			while (!(receive_line = recerive_br.readLine().trim()).equals("")) {
				receive_Hearder.add(receive_line);
			}

			char[] bodyChars = new char[1000];
			recerive_br.read(bodyChars);
			StringBuffer sb = new StringBuffer();
			sb.append(bodyChars);
			String receive_body = sb.toString().trim();

			JSONParser jp = new JSONParser();
			JSONObject requestBody = null;
			try {
				requestBody = (JSONObject) jp.parse(receive_body);
			} catch (ParseException e) {
				logger.debug(e.getMessage(), e);
			}
			dataserverID = Integer.valueOf(requestBody.get("dataserverID")
					.toString());
			currentPrimary = (JSONObject) requestBody.get("currentPrimary");
			backEndsArray = (JSONArray) requestBody.get("currentBackEndsArray");

			registerSocket.close();
			if (dataserverID == 1) {
				dataServerRole = DataServerRole.PRIMARY;
				logger.info("############ I'm  " + dataServerRole);
				logger.info("Register finish, I'm primary data server, my server ID is: "
						+ dataserverID);
				logger.info("Current primary server is: "
						+ currentPrimary.toString());
			} else {
				dataServerRole = DataServerRole.SECONDARY;
				logger.info("############ I'm  " + dataServerRole);
				logger.info("Register finish, I'm secondary data server, my server ID is: "
						+ dataserverID);
				logger.info("Current primary server is: "
						+ currentPrimary.toString());
			}
		} catch (IOException e) {
			logger.debug(e.getMessage(), e);
		}
	}

	private void acquireData() {
		logger.info("Acquiring data from primary!");
		try {
			String currentPrimaryIpAddrs = currentPrimary.get("ipAddrs")
					.toString();
			int currentPrimaryPortNum = Integer.valueOf(currentPrimary.get(
					"portNum").toString());

			Socket registerSocket = new Socket(currentPrimaryIpAddrs,
					currentPrimaryPortNum);

			BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(
					registerSocket.getOutputStream()));

			String path = "/acquireData";
			wr.write("GET " + path + " HTTP/1.1\r\n");
			wr.write("\r\n");
			wr.flush();

			BufferedReader recerive_br = new BufferedReader(
					new InputStreamReader(registerSocket.getInputStream()));

			String receive_line = null;
			ArrayList<String> receive_Hearder = new ArrayList<String>();

			while (!(receive_line = recerive_br.readLine().trim()).equals("")) {
				receive_Hearder.add(receive_line);
			}

			String[] responseHeader = null;
			responseHeader = receive_Hearder.get(0).trim().split(" ");

			String responseType = null;
			responseType = responseHeader[1];

			if (responseType.equals("200")) {
				char[] bodyChars = new char[1000];
				recerive_br.read(bodyChars);
				StringBuffer sb = new StringBuffer();
				sb.append(bodyChars);
				String receive_body = sb.toString().trim();

				JSONParser jp = new JSONParser();
				JSONObject responseBody = null;
				try {
					responseBody = (JSONObject) jp.parse(receive_body);
				} catch (ParseException e) {
					logger.debug(e.getMessage(), e);
				}

				int dataServerVersion = Integer.valueOf(responseBody.get(
						"dataServerVersion").toString());
				ds_tweetsMap.setDataServerVersion(dataServerVersion);
				JSONArray entireData = (JSONArray) responseBody
						.get("entireData");

				for (int i = 0; i < entireData.size(); i++) {
					JSONObject eachObject = (JSONObject) entireData.get(i);
					String key = eachObject.get("key").toString();
					int eachVersionNum = Integer.valueOf(eachObject.get(
							"versionNum").toString());
					JSONArray eachDataArray = (JSONArray) eachObject
							.get("dataArray");

					TweetsData tweetsData = new TweetsData();
					tweetsData.setVersionNum(eachVersionNum);
					tweetsData.setTweetsArray(eachDataArray);

					ds_tweetsMap.setTweetsHashMap(key, tweetsData);
				}

				logger.info("Acquire data from primary server successfully! My current dataServer version is: "
						+ ds_tweetsMap.getDataServerVersion());
				logger.info("My current database is: " + entireData);

			} else {
				logger.info("Fail to acquire data from primary server!");
			}

			registerSocket.close();
		} catch (IOException e) {
			logger.debug(e.getMessage(), e);
		}
	}

	public static void main(String[] args) {
		String self_ipAddrs = null;
		int self_port = Integer.valueOf(args[0]);
		String discovery_ipAddrs = null;
		try {
			self_ipAddrs = InetAddress.getLocalHost().getHostAddress();
			discovery_ipAddrs = InetAddress.getByName(args[1]).getHostAddress();
		} catch (UnknownHostException e) {
			logger.debug(e.getMessage(), e);
		}
		int discovery_port = Integer.valueOf(args[2]);
		DataServer ds = new DataServer(self_ipAddrs, self_port,
				discovery_ipAddrs, discovery_port);
		ds.registerForDiscovery();
		if (ds.dataServerRole == DataServerRole.SECONDARY) {
			// Acquire current data from primary server
			ds.acquireData();
			// Detect primary alive every 0.5 second
			Timer timer = new Timer();
			timer.scheduleAtFixedRate(ds.new DetectThread(), 0, 500);
		}
		new Thread(ds).start();
	}

	class DetectThread extends TimerTask {

		public void run() {
			if (dataServerRole == DataServerRole.SECONDARY) {
				// Start detecting primary down
				detectPrimary();
			}
		}

		private void detectPrimary() {
			String primary_ipAddrs = currentPrimary.get("ipAddrs").toString();
			int primary_portNum = Integer.valueOf(currentPrimary.get("portNum")
					.toString());
			try {
				Socket detectSocket = new Socket(primary_ipAddrs,
						primary_portNum);

				BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(
						detectSocket.getOutputStream()));

				String path = "/detection";
				wr.write("GET " + path + " HTTP/1.1\r\n");
				wr.write("\r\n");
				wr.flush();

				detectSocket.close();
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
				logger.info("Primary down, begin election###########################");
				backEndsArray.remove(currentPrimary);
				bullyElect();
			}
		}

		private void bullyElect() {
			boolean canbePrimary = true;
			for (int i = 0; i < backEndsArray.size(); i++) {
				JSONObject eachBackEnd = (JSONObject) backEndsArray.get(i);
				int each_dateserverID = Integer.valueOf(eachBackEnd.get(
						"dataserverID").toString());
				String eachBackEnd_ipAddrs = eachBackEnd.get("ipAddrs")
						.toString();
				int eachBackEnd_portNum = Integer.valueOf(eachBackEnd.get(
						"portNum").toString());
				if (each_dateserverID < dataserverID) {
					try {
						Socket electionSocket = new Socket(eachBackEnd_ipAddrs,
								eachBackEnd_portNum);

						BufferedWriter wr = new BufferedWriter(
								new OutputStreamWriter(
										electionSocket.getOutputStream()));

						String path = "/election";
						wr.write("POST " + path + " HTTP/1.1\r\n");
						wr.write("\r\n");
						wr.flush();

						BufferedReader recerive_br = new BufferedReader(
								new InputStreamReader(
										electionSocket.getInputStream()));

						while (!(recerive_br.readLine().trim()).equals("")) {
						}

						if (recerive_br.ready()) {
							char[] bodyChars = new char[1000];
							recerive_br.read(bodyChars);
							StringBuffer sb = new StringBuffer();
							sb.append(bodyChars);
							String receive_body = sb.toString().trim();

							if (receive_body.equals("You can't be primary!")) {
								logger.info("############# Receive a response from higher server, so I can't be primary!");
								canbePrimary = false;
								break;
							}
						}
						electionSocket.close();
					} catch (IOException e) {
						logger.debug(e.getMessage(), e);
						logger.info("############# No election response from dataserver "
								+ each_dateserverID);
					}
				}
			}
			if (canbePrimary == true) {
				dataServerRole = DataServerRole.PRIMARY;
				currentPrimary.put("ipAddrs", serverIpAddrs);
				currentPrimary.put("portNum", serverPort);
				currentPrimary.put("dataserverID", dataserverID);
				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					logger.debug(e.getMessage(), e);
				}
				logger.info("################## Resolve any inconsistencies in all data servers");
				consistentData();
			}
		}

		private void consistentData() {
			int oldestDataVersion = ds_tweetsMap.getDataServerVersion();
			int latestDataVersion = ds_tweetsMap.getDataServerVersion();
			String latestDataserverIpAddrs = serverIpAddrs;
			int latestDataserverPortNum = serverPort;

			// Find latest dataVersion and get latest data
			for (int i = 0; i < backEndsArray.size(); i++) {
				JSONObject eachBackEnd = (JSONObject) backEndsArray.get(i);
				int each_dateserverID = Integer.valueOf(eachBackEnd.get(
						"dataserverID").toString());
				String eachBackEnd_ipAddrs = eachBackEnd.get("ipAddrs")
						.toString();
				int eachBackEnd_portNum = Integer.valueOf(eachBackEnd.get(
						"portNum").toString());
				if (each_dateserverID != dataserverID) {
					try {
						Socket findSocket = new Socket(eachBackEnd_ipAddrs,
								eachBackEnd_portNum);

						BufferedWriter wr = new BufferedWriter(
								new OutputStreamWriter(
										findSocket.getOutputStream()));

						String path = "/latestVersion";
						wr.write("GET " + path + " HTTP/1.1\r\n");
						wr.write("\r\n");
						wr.flush();

						BufferedReader recerive_br = new BufferedReader(
								new InputStreamReader(
										findSocket.getInputStream()));

						while (!(recerive_br.readLine().trim()).equals("")) {
						}

						char[] bodyChars = new char[1000];
						recerive_br.read(bodyChars);
						StringBuffer sb = new StringBuffer();
						sb.append(bodyChars);
						String receive_body = sb.toString().trim();

						int each_DataVersion = Integer.valueOf(receive_body);

						if (each_DataVersion < oldestDataVersion) {
							oldestDataVersion = each_DataVersion;
						}

						if (each_DataVersion > latestDataVersion) {
							latestDataVersion = each_DataVersion;
							latestDataserverIpAddrs = eachBackEnd_ipAddrs;
							latestDataserverPortNum = eachBackEnd_portNum;
						}

						findSocket.close();
					} catch (IOException e) {
						logger.debug(e.getMessage(), e);
					}
				}
			}

			if (oldestDataVersion < latestDataVersion) {
				/*
				 * Notify latest version data server to consistent data to all
				 * data servers
				 */
				try {
					Socket getDataSocket = new Socket(latestDataserverIpAddrs,
							latestDataserverPortNum);
					BufferedWriter wr = new BufferedWriter(
							new OutputStreamWriter(
									getDataSocket.getOutputStream()));

					JSONObject dataRange = new JSONObject();
					dataRange.put("low", oldestDataVersion + 1);
					dataRange.put("high", latestDataVersion);

					String path = "/latestData";
					wr.write("GET " + path + " HTTP/1.1\r\n");
					wr.write("\r\n");
					wr.write(dataRange.toString());
					wr.flush();

					BufferedReader recerive_br = new BufferedReader(
							new InputStreamReader(
									getDataSocket.getInputStream()));

					while (!(recerive_br.readLine().trim()).equals("")) {
					}

					char[] bodyChars = new char[1000];
					recerive_br.read(bodyChars);
					StringBuffer sb = new StringBuffer();
					sb.append(bodyChars);
					String receive_body = sb.toString().trim();

					if (receive_body
							.equals("Notify update for all dataservers complete!")) {
						logger.info("################## Notify update for all dataservers complete!");
						logger.info("################## Notifying discovery and all secondary servers about new primary");
						notifyNewPrimary();
					}

					getDataSocket.close();
				} catch (IOException e) {
					logger.debug(e.getMessage(), e);
				}
			} else {
				logger.info("Do not need to do data consistent ######################");
				logger.info("################## Notifying discovery and all secondary servers about new primary");
				notifyNewPrimary();
			}
		}

		private void notifyNewPrimary() {

			// Notify discovery server
			try {
				Socket notifySocket = new Socket(discovery_ipAddrs,
						discovery_portNum);
				BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(
						notifySocket.getOutputStream()));

				String path = "/notifyNewPrimary";
				wr.write("POST " + path + " HTTP/1.1\r\n");
				wr.write("\r\n");
				wr.write(currentPrimary.toString());
				wr.flush();
				wr.close();
				notifySocket.close();
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			}

			// Notify all secondary servers
			for (int i = 0; i < backEndsArray.size(); i++) {
				JSONObject eachBackEnd = (JSONObject) backEndsArray.get(i);
				int each_dateserverID = Integer.valueOf(eachBackEnd.get(
						"dataserverID").toString());
				String eachBackEnd_ipAddrs = eachBackEnd.get("ipAddrs")
						.toString();
				int eachBackEnd_portNum = Integer.valueOf(eachBackEnd.get(
						"portNum").toString());
				if (each_dateserverID != dataserverID) {
					try {
						Socket notifySocket = new Socket(eachBackEnd_ipAddrs,
								eachBackEnd_portNum);

						BufferedWriter wr = new BufferedWriter(
								new OutputStreamWriter(
										notifySocket.getOutputStream()));

						String path = "/notifyNewPrimary";
						wr.write("POST " + path + " HTTP/1.1\r\n");
						wr.write("\r\n");
						wr.write(currentPrimary.toString());
						wr.flush();
						wr.close();
						notifySocket.close();
					} catch (IOException e) {
						logger.debug(e.getMessage(), e);
					}
				}
			}
		}
	}

	class DataProcessor implements Runnable {

		private Socket clientSocket = null;

		public DataProcessor(Socket clientSocket) {
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
					if (hl.getUripath().equals("tweets")) {
						String httpMethod = hl.getMethod().toString();
						if (httpMethod.equals("POST")) {
							/*
							 * Request from PostHttpServer, save the tweet into
							 * data server
							 */
							logger.info("Receive a post request from front end.................");
							storeTweet(br, hrh);
						} else if (httpMethod.equals("GET")) {
							/*
							 * Request from SearchHttpServer, check the version
							 * number and respond to SearchHttpServer
							 */
							logger.info("Receive a search request from front end.................");
							searchTweet(hl, hrh);
						} else {
							hrh.response(405, "Method Not Allowed",
									"We do not provide this method!");
							logger.info("We do not provide this method");
						}
					} else if (hl.getUripath().equals("detection")) {
						String httpMethod = hl.getMethod().toString();
						if (httpMethod.equals("GET")) {
							responseforDetection(hrh);
						} else {
							hrh.response(405, "Method Not Allowed",
									"We do not provide this method!");
							logger.info("We do not provide this method");
						}
					} else if (hl.getUripath().equals("acquireData")) {
						String httpMethod = hl.getMethod().toString();
						if (httpMethod.equals("GET")) {
							logger.info("Receive a data acquire request from a new secondary server.................");
							responseforAquireData(hrh);
						} else {
							hrh.response(405, "Method Not Allowed",
									"We do not provide this method!");
							logger.info("We do not provide this method");
						}
					} else if (hl.getUripath().equals("notifyNewBackend")) {
						String httpMethod = hl.getMethod().toString();
						if (httpMethod.equals("POST")) {
							logger.info("Receive new backend notify ##################");
							updateCurrentBackendArray(br);
						} else {
							hrh.response(405, "Method Not Allowed",
									"We do not provide this method!");
							logger.info("We do not provide this method");
						}
					} else if (hl.getUripath().equals("newData")) {
						String httpMethod = hl.getMethod().toString();
						if (httpMethod.equals("POST")) {
							logger.info("Receive a new data update from primary server ##################");
							updateData(hrh, br);
						} else {
							hrh.response(405, "Method Not Allowed",
									"We do not provide this method!");
							logger.info("We do not provide this method");
						}
					} else if (hl.getUripath().equals("election")) {
						String httpMethod = hl.getMethod().toString();
						if (httpMethod.equals("POST")) {
							logger.info("Receive a election request from a secondary server.................");
							responseforElection(hrh);
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
					} else if (hl.getUripath().equals("latestVersion")) {
						String httpMethod = hl.getMethod().toString();
						if (httpMethod.equals("GET")) {
							logger.info("Receive a latest data version compare request ##################");
							getMyLastestVersion(hrh);
						} else {
							hrh.response(405, "Method Not Allowed",
									"We do not provide this method!");
							logger.info("We do not provide this method");
						}
					} else if (hl.getUripath().equals("latestData")) {
						String httpMethod = hl.getMethod().toString();
						if (httpMethod.equals("GET")) {
							logger.info("Receive a latest data retrieve request, I need to send the latest data to other data servers ##################");
							getMyLastestData(hrh, br);
						} else {
							hrh.response(405, "Method Not Allowed",
									"We do not provide this method!");
							logger.info("We do not provide this method");
						}
					} else if (hl.getUripath().equals("updatelatestData")) {
						String httpMethod = hl.getMethod().toString();
						if (httpMethod.equals("POST")) {
							logger.info("Receive a latest data update request ##################");
							updateMyLastestData(br);
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

		private void responseforDetection(HTTPResponseHandler hrh) {
			hrh.response(200, "OK", "I'm alive!");
		}

		private void responseforAquireData(HTTPResponseHandler hrh) {
			HashMap<String, TweetsData> responseMap = ds_tweetsMap
					.getEntireMap();
			JSONObject entireData = new JSONObject();
			JSONArray entireDataArray = new JSONArray();
			for (Map.Entry<String, TweetsData> entry : responseMap.entrySet()) {
				String key = entry.getKey();
				TweetsData value = entry.getValue();
				int eachVersionNum = value.getVersionNum();
				JSONArray eachDataArray = value.getTweetsArray();
				JSONObject eachObject = new JSONObject();

				eachObject.put("key", key);
				eachObject.put("versionNum", eachVersionNum);
				eachObject.put("dataArray", eachDataArray);
				entireDataArray.add(eachObject);
			}
			entireData.put("dataServerVersion",
					ds_tweetsMap.getDataServerVersion());
			entireData.put("entireData", entireDataArray);
			hrh.response(200, "OK", entireData.toString());
			logger.info("Respond current data to the new secondary server!");
		}

		private void updateCurrentBackendArray(BufferedReader br) {
			try {
				char[] bodyChars = new char[1000];
				br.read(bodyChars);
				StringBuffer sb = new StringBuffer();
				sb.append(bodyChars);
				String postBody = sb.toString().trim();

				JSONParser jp = new JSONParser();
				JSONObject newBackendObject = (JSONObject) jp.parse(postBody);

				backEndsArray.add(newBackendObject);
				logger.info("########################## Add new backend to current backends array!");
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			} catch (ParseException e) {
				logger.debug(e.getMessage(), e);
			}
		}

		private void updateData(HTTPResponseHandler hrh, BufferedReader br) {
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

					/*
					 * Convert the post request to JSONObject, Then get the
					 * hashtags and tweet
					 */
					JSONParser jp = new JSONParser();
					JSONObject requestBody = (JSONObject) jp.parse(postBody);

					if (requestBody.containsKey("tweet")
							&& requestBody.containsKey("hashtags")) {

						logger.info("##################### Receieve a data update from primary server!");
						String tweet = requestBody.get("tweet").toString();

						JSONArray hashtags = (JSONArray) requestBody
								.get("hashtags");

						for (int i = 0; i < hashtags.size(); i++) {
							/*
							 * If the data server contains no this hashtag,
							 * create a new one in the data hashmap
							 */
							String storeTerm = hashtags.get(i).toString();
							if (!ds_tweetsMap.containsKey(hashtags.get(i)
									.toString())) {
								logger.info("Create a new hashtag in data server!");
								TweetsData tweetsData = new TweetsData();
								tweetsData.setVersionNum(0);
								tweetsData.addTweets(tweet);
								ds_tweetsMap.setTweetsHashMap(storeTerm,
										tweetsData);
								ds_tweetsMap.increaseDataServerVersion();
								// Update LogMap
								ds_tweetsMap.addLogData(
										ds_tweetsMap.getDataServerVersion(),
										tweet, hashtags);
								hrh.response(201, "Created", "Created!");
								logger.info("Successfully save the tweet into data server! My current dataVersion is: "
										+ ds_tweetsMap.getDataServerVersion());
							}
							/*
							 * If the data server contains this hashtag and does
							 * not have this tweet about this hashtag, save the
							 * tweet in and updata the version number
							 */
							else {
								logger.info("Update a hashtag in data server!");
								JSONArray tweetsArray = ds_tweetsMap
										.getTweetsArray(storeTerm);
								if (!tweetsArray.contains(tweet)) {
									int newVersionNum = ds_tweetsMap
											.getVersionNum(storeTerm) + 1;
									ds_tweetsMap.setVersionNum(storeTerm,
											newVersionNum);
									ds_tweetsMap.addTweets(storeTerm, tweet);
									ds_tweetsMap.increaseDataServerVersion();
									// Update LogMap
									ds_tweetsMap
											.addLogData(ds_tweetsMap
													.getDataServerVersion(),
													tweet, hashtags);
									hrh.response(201, "Created", "Created!");
									logger.info("Successfully save the tweet into data server! My current dataVersion is: "
											+ ds_tweetsMap
													.getDataServerVersion());
								}
							}
						}
					} else {
						hrh.response(400, "Bad Request",
								"You do not post any tweet!");
						logger.info("No tweet or no hashtag in this post request!");
					}
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

		private void responseforElection(HTTPResponseHandler hrh) {
			hrh.response(200, "OK", "You can't be primary!");
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

				currentPrimary = newPrimaryObject;
				logger.info("########################## Update current primary to the new primary: "
						+ currentPrimary);
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			} catch (ParseException e) {
				logger.debug(e.getMessage(), e);
			}
		}

		private void getMyLastestVersion(HTTPResponseHandler hrh) {
			int myLatestVersion = ds_tweetsMap.getDataServerVersion();
			String ss = String.valueOf(myLatestVersion);
			hrh.response(200, "OK", ss);
		}

		private void getMyLastestData(HTTPResponseHandler hrh, BufferedReader br) {
			try {
				char[] bodyChars = new char[1000];
				br.read(bodyChars);
				StringBuffer sb = new StringBuffer();
				sb.append(bodyChars);
				String postBody = sb.toString().trim();

				JSONParser jp = new JSONParser();
				JSONObject dataRange = (JSONObject) jp.parse(postBody);

				int lowRange = Integer.valueOf(dataRange.get("low").toString());
				int highRange = Integer.valueOf(dataRange.get("high")
						.toString());

				JSONObject logData = new JSONObject();
				for (int i = lowRange; i <= highRange; i++) {
					JSONObject logObject = new JSONObject();
					logObject.put("logTweet", ds_tweetsMap.getLogTweet(i));
					logObject.put("logHashtagsArray",
							ds_tweetsMap.getLogHashtagsArray(i));

					logData.put(i, logObject);
				}
				JSONObject logDataObject = new JSONObject();
				logDataObject.put("latestVersion", dataRange.get("high")
						.toString());
				logDataObject.put("logData", logData);

				logger.info("Respond my latest data to data servers #################");

				// Respond my latest data to data servers
				for (int i = 0; i < backEndsArray.size(); i++) {
					JSONObject eachBackEnd = (JSONObject) backEndsArray.get(i);
					int each_dateserverID = Integer.valueOf(eachBackEnd.get(
							"dataserverID").toString());
					String eachBackEnd_ipAddrs = eachBackEnd.get("ipAddrs")
							.toString();
					int eachBackEnd_portNum = Integer.valueOf(eachBackEnd.get(
							"portNum").toString());
					if (each_dateserverID != dataserverID) {
						try {
							Socket dataSocket = new Socket(eachBackEnd_ipAddrs,
									eachBackEnd_portNum);

							BufferedWriter wr = new BufferedWriter(
									new OutputStreamWriter(
											dataSocket.getOutputStream()));

							String path = "/updatelatestData";
							wr.write("POST " + path + " HTTP/1.1\r\n");
							wr.write("\r\n");
							wr.write(logDataObject.toString());
							wr.flush();
							wr.close();
							dataSocket.close();
						} catch (IOException e) {
							logger.debug(e.getMessage(), e);
						}
					}
				}
				hrh.response(200, "OK",
						"Notify update for all dataservers complete!");
			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			} catch (ParseException e) {
				logger.debug(e.getMessage(), e);
			}
		}

		private void updateMyLastestData(BufferedReader br) {
			try {
				char[] bodyChars = new char[1000];
				br.read(bodyChars);
				StringBuffer sb = new StringBuffer();
				sb.append(bodyChars);
				String postBody = sb.toString().trim();

				JSONParser jp = new JSONParser();
				JSONObject updateData = (JSONObject) jp.parse(postBody);

				int mycurrentDataVersion = ds_tweetsMap.getDataServerVersion();
				int latestVersion = Integer.valueOf(updateData.get(
						"latestVersion").toString());
				JSONObject latestData = (JSONObject) updateData.get("logData");

				for (int i = mycurrentDataVersion + 1; i <= latestVersion; i++) {
					JSONObject eachLogObject = (JSONObject) latestData
							.get(String.valueOf(i));

					String newTweet = eachLogObject.get("logTweet").toString();
					JSONArray newHashtagsArray = (JSONArray) eachLogObject
							.get("logHashtagsArray");

					for (int j = 0; j < newHashtagsArray.size(); j++) {
						String eachHashtag = newHashtagsArray.get(j).toString();
						if (!ds_tweetsMap.containsKey(eachHashtag)) {
							TweetsData tweetsData = new TweetsData();
							tweetsData.setVersionNum(0);
							tweetsData.addTweets(newTweet);
							ds_tweetsMap.setTweetsHashMap(eachHashtag,
									tweetsData);
							ds_tweetsMap.increaseDataServerVersion();
						} else {
							JSONArray tweetsArray = ds_tweetsMap
									.getTweetsArray(eachHashtag);
							if (!tweetsArray.contains(newTweet)) {
								int newVersionNum = ds_tweetsMap
										.getVersionNum(eachHashtag) + 1;
								ds_tweetsMap.setVersionNum(eachHashtag,
										newVersionNum);
								ds_tweetsMap.addTweets(eachHashtag, newTweet);
								ds_tweetsMap.increaseDataServerVersion();
							}
						}
					}
					// Update LogMap
					ds_tweetsMap.addLogData(
							ds_tweetsMap.getDataServerVersion(), newTweet,
							newHashtagsArray);
					logger.info("Successfully consistent my data, my current data version is: "
							+ ds_tweetsMap.getDataServerVersion());
					HashMap<String, TweetsData> responseMap = ds_tweetsMap
							.getEntireMap();
					JSONObject entireData = new JSONObject();
					JSONArray entireDataArray = new JSONArray();
					for (Map.Entry<String, TweetsData> entry : responseMap
							.entrySet()) {
						String key = entry.getKey();
						TweetsData value = entry.getValue();
						int eachVersionNum = value.getVersionNum();
						JSONArray eachDataArray = value.getTweetsArray();
						JSONObject eachObject = new JSONObject();

						eachObject.put("key", key);
						eachObject.put("versionNum", eachVersionNum);
						eachObject.put("dataArray", eachDataArray);
						entireDataArray.add(eachObject);
					}
					entireData.put("dataServerVersion",
							ds_tweetsMap.getDataServerVersion());
					entireData.put("entireData", entireDataArray);
					logger.info("My current database is: " + entireData);
				}

			} catch (IOException e) {
				logger.debug(e.getMessage(), e);
			} catch (ParseException e) {
				logger.debug(e.getMessage(), e);
			}
		}

		private void storeTweet(BufferedReader br, HTTPResponseHandler hrh) {
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

					/*
					 * Convert the post request to JSONObject, Then get the
					 * hashtags and tweet
					 */
					JSONParser jp = new JSONParser();
					JSONObject requestBody = (JSONObject) jp.parse(postBody);

					if (requestBody.containsKey("tweet")
							&& requestBody.containsKey("hashtags")) {

						String tweet = requestBody.get("tweet").toString();

						JSONArray hashtags = (JSONArray) requestBody
								.get("hashtags");

						for (int i = 0; i < hashtags.size(); i++) {
							/*
							 * If the data server contains no this hashtag,
							 * create a new one in the data hashmap
							 */
							String storeTerm = hashtags.get(i).toString();
							if (!ds_tweetsMap.containsKey(hashtags.get(i)
									.toString())) {
								sendNewdataToSecondary(postBody, hrh);
								logger.info("Create a new hashtag in data server!");
								TweetsData tweetsData = new TweetsData();
								tweetsData.setVersionNum(0);
								tweetsData.addTweets(tweet);
								ds_tweetsMap.setTweetsHashMap(storeTerm,
										tweetsData);
								ds_tweetsMap.increaseDataServerVersion();
								// Update LogMap
								ds_tweetsMap.addLogData(
										ds_tweetsMap.getDataServerVersion(),
										tweet, hashtags);
								logger.info("Successfully save the tweet into data server! My current dataVersion is: "
										+ ds_tweetsMap.getDataServerVersion());
							}
							/*
							 * If the data server contains this hashtag and does
							 * not have this tweet about this hashtag, save the
							 * tweet in and updata the version number
							 */
							else {
								logger.info("Update a hashtag in data server!");
								JSONArray tweetsArray = ds_tweetsMap
										.getTweetsArray(storeTerm);
								if (!tweetsArray.contains(tweet)) {
									sendNewdataToSecondary(postBody, hrh);
									int newVersionNum = ds_tweetsMap
											.getVersionNum(storeTerm) + 1;
									ds_tweetsMap.setVersionNum(storeTerm,
											newVersionNum);
									ds_tweetsMap.addTweets(storeTerm, tweet);
									ds_tweetsMap.increaseDataServerVersion();
									// Update LogMap
									ds_tweetsMap
											.addLogData(ds_tweetsMap
													.getDataServerVersion(),
													tweet, hashtags);
									logger.info("Successfully save the tweet into data server! My current dataVersion is: "
											+ ds_tweetsMap
													.getDataServerVersion());
								} else {
									hrh.response(406, "Not Acceptable",
											"This tweet exists in data server!");
									logger.info("This tweet exists in data server!");
								}
							}
						}
					} else {
						hrh.response(400, "Bad Request",
								"You do not post any tweet!");
						logger.info("No tweet or no hashtag in this post request!");
					}
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

		private void sendNewdataToSecondary(String dataObject,
				HTTPResponseHandler hrh) {
			// Send new data to all secondary servers
			boolean updateSuccessfully = true;
			for (int i = 0; i < backEndsArray.size(); i++) {
				JSONObject eachBackEnd = (JSONObject) backEndsArray.get(i);
				int each_dateserverID = Integer.valueOf(eachBackEnd.get(
						"dataserverID").toString());
				String eachBackEnd_ipAddrs = eachBackEnd.get("ipAddrs")
						.toString();
				int eachBackEnd_portNum = Integer.valueOf(eachBackEnd.get(
						"portNum").toString());
				if (each_dateserverID != dataserverID) {
					try {
						Socket notifySocket = new Socket(eachBackEnd_ipAddrs,
								eachBackEnd_portNum);

						BufferedWriter wr = new BufferedWriter(
								new OutputStreamWriter(
										notifySocket.getOutputStream()));

						String path = "/newData";
						wr.write("POST " + path + " HTTP/1.1\r\n");
						wr.write("Content-Length: " + dataObject.length()
								+ "\r\n");
						wr.write("Content-Type: application/json\r\n");
						wr.write("\r\n");
						wr.write(dataObject);
						wr.flush();

						BufferedReader recerive_br = new BufferedReader(
								new InputStreamReader(
										notifySocket.getInputStream()));
						String receive_line = null;
						ArrayList<String> receive_Hearder = new ArrayList<String>();

						String line = recerive_br.readLine();
						if (line != null) {
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

							if (!responseType.equals("201")) {
								updateSuccessfully = false;
							}
						}
						notifySocket.close();
					} catch (IOException e) {
						logger.debug(e.getMessage(), e);
					}
					// For test purpose...........................
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e) {
						logger.debug(e.getMessage(), e);
					}
				}
			}

			if (updateSuccessfully) {
				logger.info("Created tweet in all secondary servers!");
				hrh.response(201, "Created",
						"Created tweet in all secondary servers!");
			} else {
				logger.info("Sync data unsuccessfully!");
				hrh.response(503, "Service Unavailable",
						"Sync data unsuccessfully!");
			}
		}

		private void searchTweet(HTTPRequestLine hl, HTTPResponseHandler hrh) {
			HashMap<String, String> termandnum = hl.getParameters();

			/*
			 * If the parameter map is empty, that means the request contains no
			 * q
			 */
			if (termandnum.isEmpty()) {
				hrh.response(400, "Bad Request",
						"This GET request does not have q parameter!");
				logger.info("No parameter in this GET request!");
			}
			/*
			 * If the request contains no q or v, return a 400 Bad Request
			 */
			else if (!termandnum.containsKey("q")
					|| !termandnum.containsKey("v")) {
				hrh.response(400, "Bad Request",
						"Your request does not have q or v as parameters!");
				logger.info("Your request does not have q or v as parameters!");
			} else {
				/*
				 * Get the searchterm and the version number
				 */

				String search_term = termandnum.get("q");
				int search_versionNumber = Integer.valueOf(termandnum.get("v"));

				/*
				 * If data server does not have this searchterm, set the
				 * versionNum to -1 Then send a response with empty array to
				 * front end
				 */
				if (!ds_tweetsMap.containsKey(search_term)) {
					logger.info("No tweet in data server about this term!");

					JSONObject responseObject = new JSONObject();
					responseObject.put("q", search_term);
					responseObject.put("v", -1);

					JSONArray emptyArray = new JSONArray();
					responseObject.put("tweets", emptyArray);

					hrh.response(200, "OK", responseObject.toString());

				} else {
					int ds_versionNumber = ds_tweetsMap
							.getVersionNum(search_term);
					JSONArray tweetsArray = ds_tweetsMap
							.getTweetsArray(search_term);

					/*
					 * If data server contains this searchterm, and the versin
					 * num in front end is up-to-date, Then send a up-to-date
					 * response to front end
					 */
					if (ds_versionNumber == search_versionNumber) {
						hrh.response(304, "Not Modified",
								"Your version is up-to-date!");
						logger.info("Front end cache is up-to-date about this term!");
					}
					/*
					 * If the versin num in front end is not up-to-date, Then
					 * send a response with latest version number and tweets
					 * about this searchterm to front end
					 */
					else if (ds_versionNumber > search_versionNumber) {
						JSONObject responseObject = new JSONObject();
						responseObject.put("q", search_term);
						responseObject.put("v", ds_versionNumber);
						responseObject.put("tweets", tweetsArray);

						hrh.response(200, "OK", responseObject.toString());
						logger.info("Front end cache need to update about this term!");
					}
				}
			}
		}
	}
}