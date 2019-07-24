package SenapsWorkflowBeans;

import java.util.ArrayList;

public class Workflow {

	//variables
	private String workflowId="";
	private String operatorNodeId="";
	private String modelId="";
	private String executionRequestId="";
	private String executionTime="";
	private String correlationId="";
	private ArrayList<Port> connectedPorts= new ArrayList<Port>();
	
	//set functions
	public void setWorkflowId(String workflowId){
		this.workflowId=workflowId;
	}			
	public void setOperatorNodeId(String operatorNode) {
		this.operatorNodeId = operatorNode;
	}	
	public void setModelId(String modelNode) {
		this.modelId = modelNode;
	}
	public void setExecutionRequestId(String executionRequest) {
		this.executionRequestId = executionRequest;
	}
	public void setExecutionTime(String executionTime) {
		this.executionTime = executionTime;
	}
	public void setcorrelationId(String correlation) {
		this.correlationId = correlation;
	}
	public void setPortConnections(ArrayList<Port> portConnections) {
		this.connectedPorts = portConnections;
	}
	
	//getFunctions
	
	public String getWorkflowId(){
		return this.workflowId;
	}			
	public String getOperatorNodeId() {
		return this.operatorNodeId;
	}	
	public String getModelId() {
		return this.modelId;
	}
	public String getExecutionRequestId() {
		return this.executionRequestId;
	}
	public String getExecutionTime() {
		return this.executionTime;
	}
	public String getcorrelationId() {
		return this.correlationId ;
	}
	public ArrayList<Port> getPortConnections() {
		return this.connectedPorts ;
	}
	
}
