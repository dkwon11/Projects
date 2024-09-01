package buffPool;
import java.io.*;
import java.util.HashMap;
import hw1.Database;
import hw1.HeapPage;
import hw1.Tuple;


public class transactions {
	public int transID;
	public transactions(int transID){
		this.transID= transID;
	}
	
	public int getID(){
		return transID;
	}
	
	public boolean equals(Object other) { 
		if (this.hashCode() == other.hashCode()){
			return true;
		}
		return false;
	}
	public int hashCode() { 
		return transID;
	}
}
