import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@SuppressWarnings("unchecked")
public class DiscoveryData {

	private int dataserverID;
	private JSONObject currentPrimary;
	private JSONArray backEndsArray;
	
	private ThreadSafeLock primary_lock;
	private ThreadSafeLock backends_lock;
	
	public DiscoveryData(){
		dataserverID = 1;
		currentPrimary = new JSONObject();
		backEndsArray = new JSONArray();
		primary_lock = new ThreadSafeLock();
		backends_lock = new ThreadSafeLock();
	}
	
	public void addBackEnd(String ipAddrs, int portNum){
		backends_lock.lockWrite();
		if (backEndsArray.size() == 0) {
			currentPrimary.put("ipAddrs", ipAddrs);
			currentPrimary.put("portNum", portNum);
			currentPrimary.put("dataserverID", dataserverID);
		}
		JSONObject eachBackEndObject = new JSONObject();
		eachBackEndObject.put("ipAddrs", ipAddrs);
		eachBackEndObject.put("portNum", portNum);
		eachBackEndObject.put("dataserverID", dataserverID);
		backEndsArray.add(eachBackEndObject);
		dataserverID++;
		backends_lock.unlockWrite();
	}
	
	public void setCurrentPrimary(JSONObject newPrimary){
		primary_lock.lockWrite();
		currentPrimary = newPrimary;
		primary_lock.unlockWrite();
	}
	
	public void removeBackEnd(JSONObject removedBackEnd) {
		backends_lock.lockWrite();
		backEndsArray.remove(removedBackEnd);
		backends_lock.unlockWrite();
	}
	
	public int getDataserverID() {
		backends_lock.lockRead();
		int dataserverid = dataserverID;
		backends_lock.unlockRead();
		return dataserverid;
	}
	
	public JSONObject getCurrentPrimary(){
		primary_lock.lockRead();
		JSONObject currentPrimaryObject = new JSONObject();
		currentPrimaryObject.putAll(currentPrimary);
		primary_lock.unlockRead();
		return currentPrimaryObject;
	}
	
	public JSONArray getBackEndsArray(){
		backends_lock.lockRead();
		JSONArray currentBackEndsArray = new JSONArray();
		currentBackEndsArray.addAll(backEndsArray);
		backends_lock.unlockRead();
		return currentBackEndsArray;
	}
}