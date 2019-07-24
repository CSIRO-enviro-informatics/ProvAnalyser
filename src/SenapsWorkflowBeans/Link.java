package SenapsWorkflowBeans;

public class Link {
	private String outPort ="";
	private String inPort="";
 
	public void setInPort(String port){
		this.inPort= port;
	}
	
	public void setOutPort(String port){
		this.outPort= port;
	}
	
	public String getInPort(){
		return this.inPort;
	}
	
	public String getOutPort(){
		return this.outPort;
	}
	
 
 
}
