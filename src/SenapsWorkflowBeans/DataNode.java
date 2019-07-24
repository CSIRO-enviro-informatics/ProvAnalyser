package SenapsWorkflowBeans;

public class DataNode {

	private String nodeId = "";
	private String type = "";
	
	public void setNodeId(String node){
		this.nodeId =node;
	}
	
	public void setNodeType(String type){
		this.type = type;
	}
	
	public String getNodeId(){
		return this.nodeId;
	}
	
	public String getNodeType(){
		return this.type;
	}
			
}
