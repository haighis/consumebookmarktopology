package storm.consume.model;

import java.io.Serializable;

public class BookmarkUrl implements Serializable {

	private String url;
	private String bookmarkId;
	private String dtCreated;
	private String userId;
	
	public String getUserId()
	{
		return userId;
	}
	
	public String getUrl() {
         return url;
	}

	public void setUrl(String url) {
         this.url = url;
	}
	
	public String getBookmarkId()
	{
		return bookmarkId;
	}
	
	public void setBookmarkId(String bookmarkId){
		this.bookmarkId = bookmarkId;
	}
	
	public BookmarkUrl(String id, String url, String userId){
		this.bookmarkId = id;
		this.url = url;
		this.userId = userId;
	}
}
