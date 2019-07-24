package SenapsWorkflowBeans;


public class WorkflowAttribute {

	//variables
	private String workflowId="";
	private String name="";
	private String organisationId="";
	private String groupId="";
	
	//set functions
	public void setWorkflowId(String workflowId){
		this.workflowId=workflowId;
	}			
	public void setName(String name) {
		this.name = name;
	}	
	public void setOrgId(String orgId) {
		this.organisationId = orgId;
	}
	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}
	
	
	//getFunctions
	
	public String getWorkflowId(){
		return this.workflowId;
	}			
	public String getName() {
		return this.name;
	}	
	public String getOrgId() {
		return this.organisationId;
	}
	public String getGroupId() {
		return this.groupId;
	}
	
}
