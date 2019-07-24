package KnowledgeGraph;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.tdb.TDBFactory;

import AnalyticalQueries.UserDetailQuery;
import LogParser.EventLogUserData;
import SenapsWorkflowBeans.WorkflowAttribute;

public class SenapsUserDetail {


	public static String senaps = "http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#";
	public static String senapExec = "http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND/";
	public static String provone = "http://purl.dataone.org/provone/2015/01/15/ontology#";
	public static String prov = "http://www.w3.org/ns/prov#";
	public static String rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	public static String rdfs = "http://www.w3.org/2000/01/rdf-schema#";
	
	
	
	public static void main(String[] args) {
		
			  //OnDisk RDF Store
			  String directory = "C:\\Users\\but21c\\Documents\\SenapsData" ;
			  Dataset dataset = TDBFactory.createDataset(directory) ;
			  
			  //Read Ontology
			  Model model = dataset.getDefaultModel() ;
			  model.read("C:\\Users\\but21c\\Dropbox\\CSIRO_Postdoc\\ConfluxGrainsData\\SenapsOntology\\senapsLAND.owl");
			  
			  //Parse the event log and get the data in a specific format

			  String csvFile = "C:/Users/but21c/Dropbox/CSIRO_Postdoc/ConfluxGrainsData/LogData/all senaps workflows with groups 2019-05-29.csv";
		    	
			  ArrayList<WorkflowAttribute> workflows = EventLogUserData.getAllUsers(csvFile);
			  System.out.println("Total Number of Workflows : " + workflows.size());
			  
			  try
			  {
				  //Class declaration
				  Resource senapsOrg= model.getResource(senaps+"Organisation");
				  Resource senapsGroup= model.getResource(senaps+"Group");

				  
				  
				  //Property Declaration
				  
				  	//General Properties
					Property rdfTypeProperty = model.getProperty(rdf+"type");					

					Property agent = model.getProperty(prov+"agent");
					Property wasAssociatedWith = model.getProperty(prov+"wasAssociatedWith");
					

				  
					HashMap<String, String> workflowDataMap = new HashMap<String, String>();
					//
				  
				  UserDetailQuery userdata  = new UserDetailQuery();
				  int count = 1;
				  for(WorkflowAttribute workflowAttr: workflows) {
					  String workflowIds = workflowAttr.getWorkflowId();
					  System.out.println(count + "  " + workflowIds);
					  count++;
					  String org = workflowAttr.getOrgId();
					  String group = workflowAttr.getGroupId();
					  
					  workflowDataMap = userdata.getWorkflowsCount(model, workflowIds);
					  System.out.println(workflowDataMap.size());
						  
					  if (workflowDataMap.size() > 0 ){
						  for(HashMap.Entry<String, String> entry : workflowDataMap.entrySet()){ 
							  String exec = entry.getKey();
							  String assoc = entry.getValue();
							 // System.out.println(exec + "      " + assoc);
								 
							  Resource association = model.getResource(assoc);
							  Resource execution = model.getResource(exec);
								  
							  if (org.equalsIgnoreCase("")) {
							  
							  } else {
								  
								  Resource organisation = model.getResource(senaps+org);
								  organisation.addProperty(rdfTypeProperty, senapsOrg);
								  organisation.addProperty(wasAssociatedWith, execution);
								  association.addProperty(agent, organisation);
							  }
								  
							  if (group.equalsIgnoreCase("")) {
									  
							  } else {
									  
								  Resource senGroup = model.getResource(senaps+group);
								  senGroup.addProperty(rdfTypeProperty, senapsGroup);
								  senGroup.addProperty(wasAssociatedWith, execution);
								  association.addProperty(agent, senGroup);
							  }
						  }
					  }						  
				  }
				  
				  // print model
					 dataset.begin(ReadWrite.READ) ;
					 dataset.commit();
					 dataset.end() ;
			  
			  } catch (Exception e) { 
				  System.out.println("model cant print");
				  }
			  //getWorkflows(model);

		}
}
