import org.json.simple.JSONArray;

public class TweetsData {

	private int versionNum;
	private JSONArray tweetsArray = new JSONArray();

	public int getVersionNum() {
		return this.versionNum;
	}

	public JSONArray getTweetsArray() {
		return this.tweetsArray;
	}

	public void setVersionNum(int newVersionNum) {
		this.versionNum = newVersionNum;
	}

	public void setTweetsArray(JSONArray newTweetsArray) {
		this.tweetsArray = newTweetsArray;
	}
	
	@SuppressWarnings("unchecked")
	public void addTweets(String newTweet) {
		this.tweetsArray.add(newTweet);
	}
}