package buffPool;
import java.io.*;
import java.util.HashMap;
import hw1.Database;
import hw1.HeapPage;
import hw1.Tuple;


public class lock {
	public int tableId;
	public int pageId;
	Permissions perm;
	public lock(int tableId, int pageId, Permissions perm){
		this.tableId = tableId;
		this.pageId = pageId;
		this.perm = perm;
	}
	
	public Permissions getPerm() {
		return perm;
	}
	public int getTableId() {
		return tableId;
	}
	public int getPageId() {
		return pageId;
	}
	
	public void upgrade(){
		this.perm = Permissions.READ_WRITE;
	}
	
	//overrides: with these changes, two independent locks with same parameters will be identical.
	public boolean equals(Object other) { // compares hashcode and returns true if equal. Now we can use lockerRoom.containsKey(lock) with newly instantiated lock objects to check if lock already exists
		if (this.hashCode() == other.hashCode()){
			return true;
		}
		return false;
	}
	public int hashCode() { //important alteration that makes two objects with the same parameter have equal hashcodes. Also ensures same hashcode for objects with same parameter.
		return (String.valueOf(tableId) + String.valueOf(pageId) + perm.toString()).hashCode();
	}
}
