package KnowledgeGraph;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.tdb.TDBFactory;

import SenapsWorkflowBeans.AllLink;
import SenapsWorkflowBeans.Link;

public class SenapsTemplateExtraction {
	
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

		
		  ArrayList<String> workflowsList = getAllWorkflows(model);
		  ArrayList<ArrayList<String>> uniqueList = new ArrayList<ArrayList<String>>();
		  HashMap<ArrayList<String>, Integer> workflowCount = new HashMap<ArrayList<String>, Integer>();
		  
		  
		  for(String workflow:workflowsList) {
			  ArrayList<String> operatorNodesList = getAllOperatorNodesOfWorkflow(model, workflow);
			  Collections.sort(operatorNodesList);
			//  System.out.println(operatorNodesList);
			  if (uniqueList.contains(operatorNodesList)) {
				  int count = workflowCount.get(operatorNodesList);
				  //System.out.println(count);
				  workflowCount.put(operatorNodesList, count+1);
			  } else {
				  uniqueList.add(operatorNodesList);
				  
				  Timestamp timestamp = new Timestamp(System.currentTimeMillis());
     		      String id = senaps+timestamp.getTime();

     		      Resource workflowID = model.getResource(id);
     		      
     		      
				  workflowCount.put(operatorNodesList, 1);
			  }
		  }
//		  for (HashMap.Entry<ArrayList<String>, Integer> workflowCounting : workflowCount.entrySet()){
//			  System.out.println(workflowCounting.getKey() + "    " + workflowCounting.getValue());
//		  }
		  
		  for (ArrayList<String> uniqueElement: uniqueList){
			System.out.println(uniqueElement);
			int listSize= uniqueElement.size();
			AllLink[][] connections = new AllLink[listSize][listSize];
			
			//labeling the list
			HashMap<Integer, String> labels = new HashMap<Integer, String>();
			for(int i=0; i< uniqueElement.size(); i++){
				labels.put(i, uniqueElement.get(i));
			}
			
			for (HashMap.Entry<Integer, String> firstEntry : labels.entrySet()) {
			    int firstIndex = firstEntry.getKey();
			    String firstIndexValue = firstEntry.getValue();
			    // ...
			    
			    for (HashMap.Entry<Integer,String> secondEntry : labels.entrySet()) {
				    int secondIndex = secondEntry.getKey();
				    String secondIndexValue = secondEntry.getValue();
				    
				    //System.out.println( "Data Links from \" " + secondIndexValue +" \" to \" " + secondIndexValue + " \"" );
				    AllLink links = getConnectionsOfOperators(model, firstIndexValue, secondIndexValue);
				    connections[firstIndex][secondIndex] = links; 
				    			    
			    }
			    
			}
			
			for(int i=0; i < connections.length; i++) {
			
				for (int j=0; j<connections[i].length; j++) {
					
					AllLink allLinksForThis = connections[i][j];
					
					ArrayList<Link> allLinksForThisConnection = allLinksForThis.getLinks();
					
					if( allLinksForThisConnection.size() > 0) {
						System.out.println(labels.get(i) +"  --- > " + labels.get(j) + " At Ports :" ); 
						for(Link alink: allLinksForThisConnection){
							System.out.println(" OutPort  :" +alink.getOutPort() + " InPort  :" +alink.getInPort() + "	");
						}
					}
				}
			}
			
			//System.out.println(connections.length);
			
		  }
		}
		
		
		public static AllLink getConnectionsOfOperators(Model model, String firstON, String secondON){
			AllLink con = new AllLink();
			ArrayList<Link> links = new ArrayList<Link>();
			
			String queryString =
		               "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
		               "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
		               "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
		               "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
		               "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
		               "PREFIX prov:<http://www.w3.org/ns/prov#>"+
		               
		               "SELECT DISTINCT ?outPort ?inPort " +
		               "WHERE "
		               + "{?exec1 prov:qualifiedAssociation/prov:hadPlan <"+firstON+ ">."
		               +"?exec2 prov:qualifiedAssociation/prov:hadPlan <"+secondON+ ">."
		               + "?aData prov:wasGeneratedBy ?exec1. ?generation provone:hadEntity ?aData. ?generation provone:hadOutPort/senaps:portId ?outPort. ?generation rdf:type prov:Generation. "
		               + "?exec2 prov:used ?aData. ?usage provone:hadEntity ?aData. ?usage provone:hadInPort/senaps:portId ?inPort. ?usage rdf:type prov:Usage."
//		               + "?aData1 senaps:dataNodeId ?dataId1. ?aData2 senaps:dataNodeId ?dataId2. "
//		               + "FILTER (?aData1 = ?aData2) "
		               + "}";
			  
			   // System.out.println(queryString);
		        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
		        try {

		              ResultSet results = qexec.execSelect();
		              while (results.hasNext()){
			        	  Link link = new Link();
		            	  QuerySolution qs = results.next();
		            	  String outport = qs.getLiteral("?outPort").getLexicalForm();
		            	  String inport = qs.getLiteral("?inPort").getLexicalForm();
		                  //String symp = qs.getLiteral("label").getLexicalForm();
		            	  link.setOutPort(outport);
		            	  link.setInPort(inport);
		            	  
		                  links.add(link);
		              }
		            } finally {
		                qexec.close();
		             }
		        
		        con.addElementInList(links);
		        return con;
		}
		
		public static ArrayList<String> getAllWorkflows(Model model){
			ArrayList<String> workflowsList  = new ArrayList<String>(); 
			String queryString =
		               "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
		               "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
		               "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
		               "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
		               "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
		               "PREFIX prov:<http://www.w3.org/ns/prov#>"+
		               
		               "SELECT ?workflow " +
		               "WHERE "
		               + "{?workflow rdf:type senaps:Workflow.}"
		              +" ORDER BY ?workflow ";
		        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
		        try {
		              ResultSet results = qexec.execSelect();
		              // ResultSetFormatter.out(results); c49ff96d-e283-4491-bff0-cc5771b5d689
		              //ResultSetFormatter.out(System.out, results);
		              while (results.hasNext()){
		            	  QuerySolution qs = results.next();
		            	  Resource workflow = qs.getResource("?workflow");
		                  //String symp = qs.getLiteral("label").getLexicalForm();
		            	  workflowsList.add(workflow.getURI());
		                  //System.out.println(workflow);
		              }
		            } finally {
		                qexec.close();
		             }
		        return workflowsList;
		}
		
		public static ArrayList<String> getAllOperatorNodesOfWorkflow(Model model, String workflowId){
			ArrayList<String> operatorNodeList  = new ArrayList<String>(); 
			String queryString =
		               "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
		               "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
		               "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
		               "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
		               "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
		               "PREFIX prov:<http://www.w3.org/ns/prov#>"+
		               
		               "SELECT ?operatorNode " +
		               "WHERE "
		               + "{<"+workflowId+"> rdf:type senaps:Workflow; "
		               		+ "	provone:hasSubProgram ?operatorNode. "
		               		+ "?operatorNode rdf:type senaps:OperatorNode.}";
			
		        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
		        try {
		              ResultSet results = qexec.execSelect();
		              // ResultSetFormatter.out(results); c49ff96d-e283-4491-bff0-cc5771b5d689
		              //ResultSetFormatter.out(System.out, results);
		              while (results.hasNext()){
		            	  QuerySolution qs = results.next();
		            	  Resource operatorNode = qs.getResource("?operatorNode");
		                  //String symp = qs.getLiteral("label").getLexicalForm();
		            	  operatorNodeList.add(operatorNode.getURI());
		                  //System.out.println(workflow);
		              }
		            } finally {
		                qexec.close();
		             }
		        return operatorNodeList;
		}
		
}
