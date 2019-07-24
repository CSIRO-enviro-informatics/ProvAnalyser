package LogParser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import SenapsWorkflowBeans.DataNode;
import SenapsWorkflowBeans.Port;
import SenapsWorkflowBeans.Workflow;

public class EventLogParserWithTime {

    public static ArrayList<Workflow> getAllWorkflows(String csvFile) {
    	 
    	//get Only ExecutionRequestedEvent because this is the event that will give all information about the workflow
    	
    	List<String> eventsToRecord = CSVReader(csvFile);
    	ArrayList<Workflow> workflowList = new ArrayList<Workflow>();
    	
    	//System.out.println(eventsToRecord);
    	//for each event get the workflow details
    	for (int counter = 0; counter < eventsToRecord.size(); counter++){
    		
        	Workflow workflow = new Workflow();
        	
    		String[] timeTokens = eventsToRecord.get(counter).split("\\|");
    	    workflow.setExecutionTime(timeTokens[1]);
    	    //System.out.println(" TIME : " + timeTokens[1]);
    	    //System.out.println(" workflow Detail : " + timeTokens[0]);
    	    
    		String[] tokens = timeTokens[0].split(",", 6);
    		
    		for(int i=0; i<tokens.length; i++){
    	    	//System.out.println(tokens[i]);
    	    	String workflowTokenInfo =tokens[i]; 
    	    	if (workflowTokenInfo.contains("portConnections")){
    	    		String portConnections = workflowTokenInfo.split(":", 2)[1];
    	    	//	System.out.println( "portConnections  " + portConnections);
    	    	
					/*
					 * Process Detail for connected ports to this operator Node
					 * One operationNode can have multiple Ports and each port has a connected DataNode
					 * We will retrieve port Id, direction and connectedDataNode here 
					 * */
    	    		
    	    		//get each port info
    	    		String splitExpression = "\\},\\{";
    	    		String[] ports  = portConnections.split(splitExpression);
    	    		//System.out.println(ports.length);
    	    		ArrayList<Port> portsList = new ArrayList<Port>(); 
    	    		
    	    		//For each port get the details of that port
    	    		for (int portCount=0; portCount<ports.length; portCount++) {
    	    			//System.out.println(ports[portCount]);
    	    			
    	    			Port port = new Port();
    	    			//Get first 4 tokens that will return workflowId, OperatorNodeId, PortName,  rest of the String
        	    		
    	    			//System.out.println(ports[portCount]);
        	    		String[] portInfo = ports[portCount].split(",", 4);
        	    		//String portName = portInfo[1].split(",", 2)[0].replaceAll("\"", "");
        	    		
        	    		//System.out.println(  portInfo[1] + "    " + portInfo[2]);
        	    		//get PortName and add it into Port Class
        	    		if(portInfo.length==4) {
        	    			String portName = portInfo[2].split(":")[1].replaceAll("\"", "");
        	    			port.setPortId(portName);
//        	    			System.out.println( "PortName : " + portName);
        	    		
        	    			//Split "rest of the string" to get Port direction and add it into Port Class instance
//        	    			System.out.println( "portInfo[3] : " + portInfo[3]);
	        	    		String tokenToDirection = portInfo[3];
	        	    		String porDirectionToken="";
	        	    		String dataNodeInfoToken="";
	        	    		//System.out.println(tokenToDirection);
	        	    		if (tokenToDirection.contains("},")) {
	        	    			String[] portDirectionInfo = tokenToDirection.split("\\}," );     	    		
	        	    			porDirectionToken = portDirectionInfo[1];
	        	    			dataNodeInfoToken = portDirectionInfo[0]; 
	        	    		} else {
	        	    			String[] portDirectionInfo = tokenToDirection.split(",", 2 );     	    		
	        	    			porDirectionToken = portDirectionInfo[0];
	        	    			dataNodeInfoToken = portDirectionInfo[1]; 
	        	    		}
	        	    		
	        	    		String portDirection = porDirectionToken.split(":")[1].replaceAll("[,\"\\}\\]]", "");
	        	    		port.setPortDirection(portDirection);
	        	    		//System.out.println( portDirection);
	        	    		
	        	    		
	        	    		//Get connected data node of this port, One port will have only one data node connected to it
	        	    		
	        	    		//Create a DataNode Instance
	        	    		
	        	    		DataNode dataNode = new DataNode();
	        	    		
	        	    		/* From Port connection remove "connectedDataNodeId" label by splitting on ":" 
	        	    		 *  Secondly get nodeId and type from the string by splitting on ","
	        	    		 */
	        	    		//System.out.println("dataNodeInfoToken : " + dataNodeInfoToken);
	        	    		String[] dataNodeInfo = dataNodeInfoToken.split(":",2)[1].split(",");
	        	    		
	        	    		// remove "type" tag from the string and get dataNodeId by removing all "" and set it the value of dataNode class instance
	        	    		String dataNodeId = dataNodeInfo[1].split(":")[1].replaceAll("[,\"\\}\\]]", "");
	        	    		dataNode.setNodeId(dataNodeId);
	//        	    		 System.out.println(dataNodeId);
	        	    		
	        	    		// remove "documentId" tag from the string and get dataNodeId by removing all "" and set it the value of dataNode class instance
	        	    		String dataNodeType = dataNodeInfo[0].split(":")[1].replaceAll("\"", "");
	        	    		dataNode.setNodeType(dataNodeType);
	        	    		// System.out.println(dataNodeType);
	        	    		
	        	    		//Add this data node class instance into port class
	        	    		port.setDataNode(dataNode);
	        	    		portsList.add(port);
	    	    		} 	
	
	    	    		workflow.setPortConnections(portsList);
    	    		} 
    	    	} else if (workflowTokenInfo.contains("workflowId")){
    	    		String workflowId = workflowTokenInfo.split(":")[1].replaceAll("\"", "");
    	    		workflow.setWorkflowId(workflowId);
    	    		//System.out.println("workflowId " + workflowId);
    	    	} else if (workflowTokenInfo.contains("operatorNodeId")){
    	    		String operatorNodeId = workflowTokenInfo.split(":")[1].replaceAll("\"", "");
    	    		workflow.setOperatorNodeId(operatorNodeId);
    	    		//System.out.println("operatorNodeId " + operatorNodeId);
    	    	} else if (workflowTokenInfo.contains("modelId")){
    	    		String modelId = workflowTokenInfo.split(":")[1].replaceAll("\"", "");
    	    		workflow.setModelId(modelId);
    	    		//System.out.println("modelId " + modelId);
    	    	} else if (workflowTokenInfo.contains("executionRequestId")){
    	    		String executionRequestId = workflowTokenInfo.split(":")[1].replaceAll("\"", "");
    	    		workflow.setExecutionRequestId(executionRequestId);
    	    		//System.out.println("executionRequestId " + executionRequestId);
    	    	} else if (workflowTokenInfo.contains("correlationId")){
    	    		String correlationId = workflowTokenInfo.split(":")[1].replaceAll("[\"\\]\\[]", "");
    	    		workflow.setcorrelationId(correlationId);
    	    		//System.out.println("correlationId " + correlationId);    	    		
    	    	} 

    	    }
    	    
    	    workflowList.add(workflow);
    	}
  
    	return workflowList;
//    	System.out.println(workflowList.size());
    }
    
	public static List<String> CSVReader(String csvFile){
    	    	  
          BufferedReader br = null;
          String line = "";
          String cvsSplitBy = ",";
          List<String> eventsToRecord = new ArrayList<String>();
          
          try {

              br = new BufferedReader(new FileReader(csvFile));
              while ((line = br.readLine()) != null) {

                 // use comma as separator
                 String[] tokens = line.split(cvsSplitBy, 3);
                // System.out.println(tokens[0]);
                 if(tokens[1].equalsIgnoreCase("ExecutionRequestedEvent")) {
                	 //System.out.println(tokens[0]);
                	 eventsToRecord.add(tokens[2]+"|"+tokens[0]);
                 }

              }

          } catch (FileNotFoundException e) {
              e.printStackTrace();
          } catch (IOException e) {
              e.printStackTrace();
          } finally {
              if (br != null) {
                  try {
                      br.close();
                  } catch (IOException e) {
                      e.printStackTrace();
                  }
              }
          }
          return eventsToRecord;
    }
    
   
    

}