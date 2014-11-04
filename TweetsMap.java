import java.util.HashMap;

import org.json.simple.JSONArray;

@SuppressWarnings("unchecked")
public class TweetsMap {

	private HashMap<String, TweetsData> tweetsHashMap;
	private HashMap<Integer, TweetsLog> logMap;
	private ThreadSafeLock lock;
	private ThreadSafeLock log_lock;
	private int dataServerVersion;

	public TweetsMap() {
		tweetsHashMap = new HashMap<String, TweetsData>();
		logMap = new HashMap<Integer, TweetsLog>();
		lock = new ThreadSafeLock();
		log_lock = new ThreadSafeLock();
		dataServerVersion = 0;
	}

	public boolean containsKey(String key) {
		lock.lockRead();
		boolean containsKey = tweetsHashMap.containsKey(key);
		lock.unlockRead();
		return containsKey;
	}

	public int getDataServerVersion() {
		lock.lockRead();
		int version = dataServerVersion;
		lock.unlockRead();
		return version;
	}

	public int getVersionNum(String key) {
		lock.lockRead();
		int versionNum = tweetsHashMap.get(key).getVersionNum();
		lock.unlockRead();
		return versionNum;
	}

	public JSONArray getTweetsArray(String key) {
		lock.lockRead();
		JSONArray tweetsArray = new JSONArray();
		tweetsArray.addAll(tweetsHashMap.get(key).getTweetsArray());
		lock.unlockRead();
		return tweetsArray;
	}

	public HashMap<String, TweetsData> getEntireMap() {
		lock.lockRead();
		HashMap<String, TweetsData> responseMap = new HashMap<String, TweetsData>();
		responseMap.putAll(tweetsHashMap);
		lock.unlockRead();
		return responseMap;
	}

	public void increaseDataServerVersion() {
		lock.lockWrite();
		dataServerVersion++;
		lock.unlockWrite();
	}

	public void setDataServerVersion(int newVersion) {
		lock.lockWrite();
		dataServerVersion = newVersion;
		lock.unlockWrite();
	}

	public void setTweetsHashMap(String key, TweetsData tweetsData) {
		lock.lockWrite();
		tweetsHashMap.put(key, tweetsData);
		lock.unlockWrite();
	}

	public void setVersionNum(String key, int newVersionNum) {
		lock.lockWrite();
		tweetsHashMap.get(key).setVersionNum(newVersionNum);
		lock.unlockWrite();
	}

	public void setTweetsArray(String key, JSONArray newTweetsArray) {
		lock.lockWrite();
		tweetsHashMap.get(key).setTweetsArray(newTweetsArray);
		lock.unlockWrite();
	}

	public void addTweets(String key, String newTweet) {
		lock.lockWrite();
		tweetsHashMap.get(key).addTweets(newTweet);
		lock.unlockWrite();
	}
	
	// TweetsLog operations
	public String getLogTweet(Integer dataVersion) {
		log_lock.lockRead();
		String logTweet = logMap.get(dataVersion).getTweet();
		log_lock.unlockRead();
		return logTweet;
	}
	
	public JSONArray getLogHashtagsArray(Integer dataVersion) {
		log_lock.lockRead();
		JSONArray returnArray = new JSONArray();
		returnArray.addAll(logMap.get(dataVersion).getHashtagsArray());
		log_lock.unlockRead();
		return returnArray;
	}

	public void addLogData(Integer dataVersion, String newTweet, JSONArray newHashtagsArray) {
		log_lock.lockWrite();
		TweetsLog tl = new TweetsLog();
		tl.addData(newTweet, newHashtagsArray);
		logMap.put(dataVersion, tl);
		log_lock.unlockWrite();
	}
	
	class TweetsLog {
		private String tweet;
		private JSONArray hashtagsArray;

		public TweetsLog() {
			tweet = null;
			hashtagsArray = new JSONArray();
		}
		
		public String getTweet() {
			String returnTweet = tweet;
			return returnTweet;
		}
		
		public JSONArray getHashtagsArray(){
			JSONArray returnArray = new JSONArray();
			returnArray.addAll(hashtagsArray);
			return returnArray;
		}
		
		public void addData(String newTweet, JSONArray newHashtagsArray) {
			tweet = newTweet;
			hashtagsArray.addAll(newHashtagsArray);
		}
	}
}