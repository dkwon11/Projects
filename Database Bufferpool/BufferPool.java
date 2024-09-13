package buffPool;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import hw1.Database;
import hw1.HeapFile;
import hw1.HeapPage;
import hw1.Tuple;

/**
 * David Kwon  
 * BufferPool Program
 * For data structures, I use an ArrayList to keep track of my cache pages and a HashMap to store my lock & transacionID relation. Note that 
 * as we can have multiple transactions for a given lock (shared lock for reads), the transacionID in HashMap is stored in an ArrayList.
 * As a means to prevent deadlock, I used a timer that will abort the transaction if it reaches the time threshold of 7 seconds. I sleep the thread for 1 second
 * between each lock attempts to reduce total computation. 
 */
public class BufferPool {
    public static final int PAGE_SIZE = 4096;
    public static final int DEFAULT_PAGES = 50;
    public static final long MAX_TIME = 7000; 
    public static final int SLEEP_TIME = 1000;
    private int numPages;
    private ArrayList<HeapPage> pageCache;
	public HashMap<lock, ArrayList<Integer>> lockerRoom;
    
    public BufferPool(int numPages) {
    	this.numPages = numPages;
		this.lockerRoom = new HashMap<lock, ArrayList<Integer>>();
		this.pageCache = new ArrayList<HeapPage>();
    }
    
    public boolean checkLocks(int transId, lock newLock) {
    	Permissions perm = newLock.getPerm();
    	int tableId = newLock.getTableId();
    	int pid = newLock.getPageId();
    	if (perm == Permissions.READ_ONLY) { //if I want to create a new read lock, I have to make sure there isn't any write lock associated with the page
    		lock writeLock = new lock(tableId, pid, Permissions.READ_WRITE);
    		if (lockerRoom.containsKey(writeLock)) {
    			ArrayList<Integer> transList = lockerRoom.get(writeLock);
    			for (Integer i : transList) {
    				if (transId != i) { 
    					return false; //a write lock already exists, so can't lock. return false
    				}
    			}
    		}
    	} else if (perm == Permissions.READ_WRITE) {
    		lock writeLock = new lock(tableId, pid, Permissions.READ_WRITE);
    		lock readLock = new lock(tableId, pid, Permissions.READ_ONLY);
    		
    		if (lockerRoom.containsKey(writeLock)){ //checking if another transaction has a write lock 
    			ArrayList<Integer> transList = lockerRoom.get(writeLock);
    			for (Integer i : transList){
    				if (transId != i){
    					return false;
    				}
    			}
    		} 
    		if (lockerRoom.containsKey(readLock)){ //checking if another transaction has a read lock
    			ArrayList<Integer> transList2 = lockerRoom.get(readLock);
    			for (Integer i : transList2){
    				if (transId != i){
    					return false;
    				}
    			}
    		}
    	}
    	return true; //if the code reaches here, lock can be obtained!
    }
    
    public boolean tryLock(int transId, lock newLock) {
    	if (holdsSpecificLock(transId, newLock)){ //if same lock is already held, simply return true
    		return true;
    	}
    	if (!checkLocks(transId, newLock)) {
    		return false;
    	}
    	if (newLock.getPerm() == Permissions.READ_ONLY){ //case when we request a read lock on a transaction that already has a write lock on the page. simply return true.
    		lock writeLock = new lock(newLock.getTableId(), newLock.getPageId(), Permissions.READ_WRITE);
    		if (holdsSpecificLock(transId, writeLock)){
    			return true;
    		}
    	}
    	ArrayList<Integer> transList;
    	if (newLock.getPerm() == Permissions.READ_WRITE){ //case when we need to upgrade a read lock into a write lock for a given transaction and page.
    		lock readLock = new lock(newLock.getTableId(), newLock.getPageId(), Permissions.READ_ONLY);
    		if (lockerRoom.containsKey(readLock)){ 
    			ArrayList<Integer> tempList= lockerRoom.get(readLock);
    			for (Integer i : tempList){ //we should only have 1 transaction holding that read lock as we would've returned false in checkLocks() if otherwise. I still iterate to be safe. 
    				if (transId == i){
    					lockerRoom.get(readLock).remove(Integer.valueOf(transId)); //using Integer.valueof to guide the .remove method to (object o), instead of (int index) due to transId being an int
    					if (lockerRoom.get(readLock).isEmpty()){ //if there are no more transactions for that readlock, remove
    						lockerRoom.remove(readLock);
    					}
    					break;
    				}
    			}
    			
    		}
    	}
    	if (lockerRoom.containsKey(newLock)) {
    		transList = lockerRoom.get(newLock);
    		transList.add(transId);
    		lockerRoom.put(newLock, transList);
    	} else {
    		transList = new ArrayList<Integer>(); //if lock doesn't exist, we create the necessary list for new entry
    		transList.add(transId);
    		lockerRoom.put(newLock, transList);
    	}
    	return true;
    }
    
    public void abortTransaction (int transId) throws IOException{
    	ArrayList<lock> lockList = getLocksByTransaction(transId);
    	if (!lockList.isEmpty()){
    		for (lock l : lockList){
    			rollbackPages(l.getTableId(), l.getPageId());
    			releasePage(transId, l.getTableId(), l.getPageId());
    		}
    	}
    }
    
    public synchronized boolean tryMore (int transId, lock newLock) throws InterruptedException{ //try more until deadlock is suspected. 
    	long startTime = System.currentTimeMillis();
    	while (!tryLock (transId, newLock)){
    		Thread.sleep(SLEEP_TIME); //going to check every 1 second
    		long currentTime= System.currentTimeMillis();
    		if (currentTime - startTime >= MAX_TIME){ //Going to attempt getting the lock for a total of 7 seconds. If lock hasn't been released by then, return false so we can abort.
    			System.out.println("Deadlock has occured for transaction " + transId + ".");
    			return false;
    		}
    	}
    	return true; //if code reaches this point, lock can now be acquired.
    }
    
    public HeapPage getPage(int tid, int tableId, int pid, Permissions perm)
        throws Exception {
    	lock newLock= new lock(tableId, pid, perm);
    	boolean lockResult = tryLock(tid, newLock); //will check if lock has been successfully acquired. false if unsuccessful, so abort.
    	if (!lockResult) {
    		if (!tryMore(tid,newLock)){ //try more lock attempts in case lock is released by other transaction. If it's still not released, we abort the transaction.
    			abortTransaction (tid);
        		System.out.println("Aborted transaction number " + tid + ".");
        		return null;
    		}
    	}
    	HeapPage targetPage = Database.getCatalog().getDbFile(tableId).readPage(pid);
    	if (!pageCache.contains(targetPage)){ //if this conditional fails, it means that the page is already in our cache.
    		if (pageCache.size() >= numPages){ //when we run out of space, we call evict to make space for the new page.
    			try{
    				evictPage();
    			} catch (Exception e){
    				abortTransaction(tid);
    			}
    		}
    		pageCache.add(targetPage);
    	}
        return targetPage;
    }

    public void releasePage(int tid, int tableId, int pid) {
    	lock readLock = new lock(tableId, pid, Permissions.READ_ONLY);
    	lock writeLock = new lock(tableId, pid, Permissions.READ_WRITE);
    	ArrayList<Integer> readTransactions = lockerRoom.get(readLock);
    	ArrayList<Integer> writeTransactions = lockerRoom.get(writeLock);
    	if (readTransactions != null){
        	for (int r : readTransactions){
        		if (tid == r){
        			lockerRoom.get(readLock).remove(Integer.valueOf(tid));
        			if (lockerRoom.get(readLock).isEmpty()){
        				lockerRoom.remove(readLock);
        			}
        			break; //need to break out of for loop to avoid concurrent modification error
        		}
        	}
    	}
    	if (writeTransactions != null){
        	for (int w : writeTransactions){
        		if (tid == w){
        			lockerRoom.get(writeLock).remove(Integer.valueOf(tid));
        			if (lockerRoom.get(writeLock).isEmpty()){
        				lockerRoom.remove(writeLock);
        			}
        			break;
        		}
        	}
    	}
    }
    
    public boolean holdsSpecificLock(int tid, lock l) { //checking if a transaction has a specific lock.
    	if (l.getPerm() == Permissions.READ_ONLY){
    		lock readLock = new lock(l.getTableId(), l.getPageId(), Permissions.READ_ONLY);
    		ArrayList<Integer> readTransactions = lockerRoom.get(readLock);
    		if (readTransactions != null){
            	for (int r : readTransactions){
            		if (tid == r){
            			return true;
            		}
            	}
        	}
    	} else{
    		lock writeLock = new lock(l.getTableId(), l.getPageId(), Permissions.READ_WRITE);
    		ArrayList<Integer> writeTransactions = lockerRoom.get(writeLock);
    		if (writeTransactions != null){
            	for (int w : writeTransactions){
            		if (tid == w){
            			return true;
            		}
            	}
        	}
    	}
        return false; //if code reaches here, lock has not been acquired for that page for a given transaction.
    }
    
    public boolean holdsLock(int tid, int tableId, int pid) { //general version of holdsSpecificLock - if there is any lock associated with the transaction and page, return true.
    	lock readLock = new lock(tableId, pid, Permissions.READ_ONLY);
    	lock writeLock = new lock(tableId, pid, Permissions.READ_WRITE);
    	ArrayList<Integer> readTransactions = lockerRoom.get(readLock);
    	ArrayList<Integer> writeTransactions = lockerRoom.get(writeLock);
    	if (readTransactions != null){
        	for (int r : readTransactions){
        		if (tid == r){
        			return true;
        		}
        	}
    	}
    	if (writeTransactions != null){
        	for (int w : writeTransactions){
        		if (tid == w){
        			return true;
        		}
        	}
    	}
        return false; //if code reaches here, lock has not been acquired for that page for a given transaction.
    }

    public ArrayList<lock> getLocksByTransaction(int tid){ //helper function that will yield a arraylist of locks associated with a transaction.
    	ArrayList<lock> transLock = new ArrayList<lock>();
    	for (lock l : lockerRoom.keySet()){
    		int tableId = l.getTableId();
    		int pageId = l.getPageId();
    		if (holdsLock(tid, tableId, pageId)){
    			transLock.add(l);
    		}
    	}
    	return transLock;
    }
    
    public void rollbackPages (int tableId, int pageId) throws IOException{ //rollback changes to disk page if cache page is dirty.
    	byte[] tempByte = new byte[PAGE_SIZE];
    	HeapPage originalPage = new HeapPage(pageId, tempByte, tableId);
    	int pageIndex = pageCache.indexOf(originalPage); //-1 if not found
    	if (pageIndex == -1){
    		System.out.println("Rollback Failure - page not found in cache");
    		return;
    	}
    	if (pageCache.get(pageIndex).checkDirty()){
        	pageCache.remove(pageIndex);
        	pageCache.add(originalPage);
    	}
    }
    
    public void transactionComplete(int tid, boolean commit)
        throws IOException {
    	ArrayList<lock> lockList= getLocksByTransaction(tid);
    	if (lockList.isEmpty()){
    		System.out.println("Transaction " + tid + " doesn't have any associated locks");
    		return;
    	}
    	if (commit){
    		for (lock l : lockList){
    			flushPage(l.tableId, l.pageId);
    			releasePage(tid,l.tableId,l.pageId);
    		}
    	} else{
    		for (lock l : lockList){
    			rollbackPages(l.tableId, l.pageId);
    			releasePage(tid,l.tableId,l.pageId);
    		}
    	}  	
    }

    public  void insertTuple(int tid, int tableId, Tuple t)
        throws Exception {
    	HeapFile targetFile = Database.getCatalog().getDbFile(tableId);
    	HeapPage newPage= targetFile.addTuple(tid, t, numPages);
    	if (newPage == null){ //lock has failed, so exit this function so that the other can proceed.
    		return;
    	}
    	newPage.makeDirty(); //page has been modified - make it dirty.
    	if (pageCache.contains(newPage)){ //if same page is already cached, update with modified page
    		pageCache.remove(newPage); 
    		pageCache.add(newPage);
    	} else{
    		if (pageCache.size() >= numPages){ //check if there's space
    			try{
    				evictPage();
    			} catch (Exception e){
    				abortTransaction(tid);
    			}
    		}
    		pageCache.add(newPage);
    	}
    }

    public  void deleteTuple(int tid, int tableId, Tuple t)
        throws Exception {
    	HeapFile targetFile = Database.getCatalog().getDbFile(tableId);
    	HeapPage newPage= targetFile.deleteTuple(tid, t);
    	if (newPage == null){
    		return;
    	}
    	newPage.makeDirty();
    	if (pageCache.contains(newPage)){
    		pageCache.remove(newPage);
    		pageCache.add(newPage);
    	} else{
    		if (pageCache.size()>=numPages){
    			try{
    				evictPage();
    			} catch (Exception e){
    				abortTransaction(tid);
    			}
    		}
    		pageCache.add(newPage);
    	}
    }

    private synchronized  void flushPage(int tableId, int pid) throws IOException {
    	byte[] tempByte = new byte[PAGE_SIZE];
    	HeapPage tempPage = new HeapPage(pid, tempByte, tableId);
    	int pageIndex = pageCache.indexOf(tempPage); //-1 if not found
    	if (pageIndex == -1){ //checking if page exists in cache. if this is true, it implies that our page is clean and has been deleted from cache from eviction.
    		System.out.println("Flush Failure - page not found in cache");
    		return;
    	}
    	HeapPage cachePage = pageCache.get(pageIndex);
    	if (cachePage.checkDirty()){ //if page has been modified, write to disk and make clean again.
    		System.out.println("Writing to Disk -> TableId: " + tableId + ", PageId: " + pid + ".");
    		Database.getCatalog().getDbFile(tableId).writePage(cachePage);
    		cachePage.makeClean();
    	}
    }

    private synchronized  void evictPage() throws Exception {
        for (HeapPage hp : pageCache){
        	if (!hp.checkDirty()){ //check if clean
        		pageCache.remove(hp);
        		return;
        	}
        }
        System.out.println("Eviction failed! No pages are clean");
        throw new Exception();
    }

}
