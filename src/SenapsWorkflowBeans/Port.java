package SenapsWorkflowBeans;

public class Port {
	
	private String portId = "";
	private String portDirection = "";
	private DataNode connectedDataNode = new DataNode();
	
	public void setPortId(String port){
		this.portId = port;
	}
	
	public void setPortDirection(String direction){
		this.portDirection = direction;
	}
	
	public void setDataNode(DataNode dataNode){
		this.connectedDataNode = dataNode;
	}
	
	
	public String getPortId(){
		return this.portId;
	}
	
	public String getPortDirection(){
		return this.portDirection;
	}
	
	public DataNode getDataNode(){
		return this.connectedDataNode;
	}
}
