package KnowledgeGraph;

import java.util.ArrayList;

import org.apache.jena.graph.Graph;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.tdb.TDBFactory;


import LogParser.EventLogParserWithTime;
import SenapsWorkflowBeans.DataNode;
import SenapsWorkflowBeans.Port;
import SenapsWorkflowBeans.Workflow;

public class SenapsDataModel {


	public static String senaps = "http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#";
	public static String senapExec = "http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND/";
	public static String provone = "http://purl.dataone.org/provone/2015/01/15/ontology#";
	public static String prov = "http://www.w3.org/ns/prov#";
	public static String rdf = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	public static String rdfs = "http://www.w3.org/2000/01/rdf-schema#";
	
	
	
	public static void main(String[] args) {
		
			  //OnDisk RDF Store
			  String directory = "C:\\Users\\but21c\\Documents\\SenapsDataPerformanceTest" ;
			  Dataset dataset = TDBFactory.createDataset(directory) ;
			  
			  //Read Ontology
			  Model model = dataset.getDefaultModel() ;
			  model.read("C:\\Users\\but21c\\Dropbox\\CSIRO_Postdoc\\ConfluxGrainsData\\SenapsOntology\\senapsLAND.owl");
			  
			  //Parse the event log and get the data in a specific format
			  EventLogParserWithTime parser = new EventLogParserWithTime();
			  String csvFile = "C:/Users/but21c/Dropbox/CSIRO_Postdoc/ConfluxGrainsData/LogData/MyData/xas.csv";
		    
			  System.out.println(System.currentTimeMillis());
			  
			  ArrayList<Workflow> workflows = parser.getAllWorkflows(csvFile);
			  System.out.println("Total Number of Workflows : " + workflows.size());
			  
			  try
			  {
				  //Class declaration
				  Resource senapsWorkflow= model.getResource(senaps+"Workflow");
				  Resource senapsOperatorNode= model.getResource(senaps+"OperatorNode");
				  Resource senapsModel= model.getResource(senaps+"Model");
				  Resource senapsPort= model.getResource(senaps+"Port");
				  Resource senapsDataNode= model.getResource(senaps+"DataNode");
				  Resource senapsOperatorNodeExecution= model.getResource(senaps+"OperatorNodeExecution");
				  Resource provAssociation= model.getResource(prov+"Association");
				  Resource provUsage= model.getResource(prov+"Usage");
				  Resource provGeneration= model.getResource(prov+"Generation");
				  
				  
				  //Property Declaration
				  
				  	//General Properties
					Property rdfTypeProperty = model.getProperty(rdf+"type");
					
					// Associations
					
					Property hasSubProgram = model.getProperty(provone+"hasSubProgram");
					Property hasInPort = model.getProperty(provone+"hasInPort");
					Property hasOutPort = model.getProperty(provone+"hasOutPort");
					Property host = model.getProperty(senaps+"host");
					Property connectsTo = model.getProperty(provone+"connectsTo");
					Property hadInPort = model.getProperty(provone+"hadInPort");
					Property hadOutPort = model.getProperty(provone+"hadOutPort");
					Property hadEntity = model.getProperty(provone+"hadEntity");
					
					Property qualifiedAssociation = model.getProperty(prov+"qualifiedAssociation");
					Property hadPlan = model.getProperty(prov+"hadPlan");
					Property agent = model.getProperty(prov+"agent");
					Property wasAssociatedWith = model.getProperty(prov+"wasAssociatedWith");
					Property qualifiedUsage = model.getProperty(prov+"qualifiedUsage");
					Property qualifiedGeneration = model.getProperty(prov+"qualifiedGeneration");
					Property used = model.getProperty(prov+"used");
					Property wasGeneratedBy = model.getProperty(prov+"wasGeneratedBy");
								
					
					// Properties
					Property senapsWorkflowId = model.getProperty(senaps+"workflowId");
					Property senapsOperatorNodeId = model.getProperty(senaps+"operatorNodeId");
					Property senapsModelId = model.getProperty(senaps+"modelId");
					Property senapsPortId = model.getProperty(senaps+"portId");
					Property senapsDataNodeId = model.getProperty(senaps+"dataNodeId");
					Property senapsExecutionId = model.getProperty(senaps+"executionId");
					Property senapsExecutionRequestTime = model.getProperty(prov+"atTime");
				  
				  
				  
				  for(Workflow workflow : workflows) {
					  
					  
					  	/** 
					  	 *  Add the workflow detail
					  	 **/
					  
						String workflowId = workflow.getWorkflowId();
						
						Resource workflowInstance = model.getResource(senaps+workflowId);
						workflowInstance.addProperty(rdfTypeProperty, senapsWorkflow);
						workflowInstance.addProperty(senapsWorkflowId, model.createLiteral(workflowId));	
						
						
						String correlationId = workflow.getcorrelationId();
						//System.out.println("correlationId : "+correlationId);
						
						
						/** 
						 * Add Operator Node details
						 **/
						
						String operatorNodeId = workflow.getOperatorNodeId();
						Resource operatorNodeInstance = model.getResource(senaps+operatorNodeId); 
						
						//add properties
						operatorNodeInstance.addProperty(rdfTypeProperty, senapsOperatorNode); //declare type of operator node
						operatorNodeInstance.addProperty(senapsOperatorNodeId, model.createLiteral(operatorNodeId)); // add operator node id	
						
						//add association
						workflowInstance.addProperty(hasSubProgram, operatorNodeInstance); // link operator node with workflow
						
						
						/** 
						 * Add Model of OperatorNode
						 **/
						
						String modelId = workflow.getModelId();

						Resource modelInstance = model.getResource(senaps+modelId); 
						
						//add properties
						modelInstance.addProperty(rdfTypeProperty, senapsModel); //declare type of model
						modelInstance.addProperty(senapsModelId, model.createLiteral(modelId)); // add model id	
						
						//add association
						operatorNodeInstance.addProperty(host, modelInstance); // link model with operator node
						
						/** 
						 * Add Execution Request details
						 **/
						
						String executionRequestId = workflow.getExecutionRequestId();
						Resource executionRequestInstance = model.getResource(senaps+executionRequestId); 
						Resource associationInstance = model.getResource(senapExec+"association/"+executionRequestId); // create association for this execution on prov namespace with same id
						
						//add properties
						executionRequestInstance.addProperty(rdfTypeProperty, senapsOperatorNodeExecution); //declare type of operator node
						executionRequestInstance.addProperty(senapsExecutionId, model.createLiteral(executionRequestId)); // add operator node id	
						executionRequestInstance.addProperty(senapsExecutionRequestTime, workflow.getExecutionTime());
						associationInstance.addProperty(rdfTypeProperty, provAssociation);
						
						//add association
						executionRequestInstance.addProperty(qualifiedAssociation, associationInstance); // link operator node with workflow
						associationInstance.addProperty(hadPlan, operatorNodeInstance);
						//////// ADD USERS HEREEE
						
						
						
						/** 
						 * Add Ports and datanodes of OperatorNode
						 **/
						
						ArrayList<Port> ports = workflow.getPortConnections();
						
						for(Port port : ports){
							String portId = port.getPortId();
							String portDirection = port.getPortDirection();
							DataNode connectedDataNode = port.getDataNode();
							String dataNodeId = connectedDataNode.getNodeId();
							String dataNodeType = connectedDataNode.getNodeType();
							
							//Add port first
							
							Resource portInstance = model.getResource(senaps+operatorNodeId+"_"+portId); 
							
							//add properties
							portInstance.addProperty(rdfTypeProperty, senapsPort); //declare type of Port
							portInstance.addProperty(senapsPortId, model.createLiteral(portId)); // add port id	
							
							//Now Add DataNode
							Resource dataNodeInstance = model.getResource(senaps+dataNodeId); 
							dataNodeInstance.addProperty(senapsDataNodeId, dataNodeId);
							
					
							//add association
							if(portDirection.equalsIgnoreCase("Input")){
								operatorNodeInstance.addProperty(hasInPort, portInstance); // link model with operator node
								
								//workflow execution detail
								Resource usage = model.getResource(senapExec+"usage/"+portId+"/"+executionRequestId);
								usage.addProperty(rdfTypeProperty, provUsage);
								executionRequestInstance.addProperty(qualifiedUsage, usage);
								usage.addProperty(hadInPort, portInstance);
								usage.addProperty(hadEntity, dataNodeInstance);
								executionRequestInstance.addProperty(used, dataNodeInstance);
								
							} else if(portDirection.equalsIgnoreCase("Output")){
								operatorNodeInstance.addProperty(hasOutPort, portInstance); // link model with operator node
								
								//workflow execution detail
								Resource generation = model.getResource(senapExec+"generation/"+portId+"/"+executionRequestId);
								generation.addProperty(rdfTypeProperty, provGeneration);
								executionRequestInstance.addProperty(qualifiedGeneration, generation);
								generation.addProperty(hadOutPort, portInstance);
								generation.addProperty(hadEntity, dataNodeInstance);
								dataNodeInstance.addProperty(wasGeneratedBy, executionRequestInstance);
							}
								
							
							/** 
							 * Add Data nodes for this port
							 **/
							
							if (dataNodeType.equalsIgnoreCase("STREAM")){
								Resource dataNodeTypeEntity = model.getResource(senaps+"Stream");
								portInstance.addProperty(connectsTo, dataNodeTypeEntity); //connect to a datanode
								dataNodeInstance.addProperty(rdfTypeProperty, dataNodeTypeEntity); //declare type of Port
							} else if (dataNodeType.equalsIgnoreCase("DOCUMENT")){
								Resource dataNodeTypeEntity = model.getResource(senaps+"Document");
								portInstance.addProperty(connectsTo, dataNodeTypeEntity); //connect to a datanode
								dataNodeInstance.addProperty(rdfTypeProperty, dataNodeTypeEntity); 
							} else if (dataNodeType.equalsIgnoreCase("GRID")){
								Resource dataNodeTypeEntity = model.getResource(senaps+"Grid");
								portInstance.addProperty(connectsTo, dataNodeTypeEntity); //connect to a datanode
								dataNodeInstance.addProperty(rdfTypeProperty, dataNodeTypeEntity); 
							} else if (dataNodeType.equalsIgnoreCase("DATA")){
								Resource dataNodeTypeEntity = model.getResource(senaps+"Data");
								portInstance.addProperty(connectsTo, dataNodeTypeEntity); //connect to a datanode
								dataNodeInstance.addProperty(rdfTypeProperty, dataNodeTypeEntity); 
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

			  System.out.println(System.currentTimeMillis());
		}
}
