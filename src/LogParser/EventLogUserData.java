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
import SenapsWorkflowBeans.WorkflowAttribute;

public class EventLogUserData {

    public static void main(String[] args) {
    	String csvFile = "C:/Users/but21c/Dropbox/CSIRO_Postdoc/ConfluxGrainsData/LogData/all senaps workflows with groups 2019-05-29.csv";
    	ArrayList<WorkflowAttribute> workflows = getAllUsers(csvFile);
    	System.out.println(workflows.size());
    }
    
	public static ArrayList<WorkflowAttribute> getAllUsers(String csvFile){
    	    	  
          BufferedReader br = null;
          String line = "";
          String cvsSplitBy = ",";
          ArrayList<WorkflowAttribute> workflows = new ArrayList<WorkflowAttribute>();
          try {

              br = new BufferedReader(new FileReader(csvFile));
              while ((line = br.readLine()) != null) {
            	  WorkflowAttribute workflow = new WorkflowAttribute();
                 // use comma as separator
                 String[] tokens = line.split(cvsSplitBy, 4);
                
                	 workflow.setWorkflowId(tokens[0]);
                	 workflow.setName(tokens[1]);
                	 workflow.setOrgId(tokens[2]);
                	 workflow.setGroupId(tokens[3]);
                	 //System.out.println(workflow.getWorkflowId());
                	 workflows.add(workflow);
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
          return workflows;
    }
    
   
    

}