package sparql2impala.mapreduce.util;

public class Index {
	int[] maxValues;
	int[] index; 
	
	public Index(int[] maxValues){
		this.maxValues = maxValues;
		this.index= new int[maxValues.length];
	}
	
	public int[] getIndexValue(){
		return this.index;
	}
	
	
	
	public boolean increment(){
		boolean result = false; 
		for(int i = index.length-1;i>=0; i--){
			if(index[i]+1< maxValues[i]){
				index[i]++;
				result = true;
				break;
			} else{
				index[i] = 0;
			}
		}
		return result;
	}
	
}
