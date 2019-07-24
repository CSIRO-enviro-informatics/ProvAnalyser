package AnalyticalQueries;
import java.util.ArrayList;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.tdb.TDBFactory;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.ReadWrite;

import SenapsWorkflowBeans.DataNode;
import SenapsWorkflowBeans.Port;
import SenapsWorkflowBeans.Workflow;


public class SenapsQueryDataFormats {
	public static void main(String[] args) {
		
		  //OnDisk RDF Store
		  String directory = "C:\\Users\\but21c\\Documents\\SenapsData" ;
		  Dataset dataset = TDBFactory.createDataset(directory) ;
		  
		  //Read Ontology
		  Model model = dataset.getDefaultModel() ;
		  model.read("C:\\Users\\but21c\\Dropbox\\CSIRO_Postdoc\\ConfluxGrainsData\\SenapsOntology\\senapsLAND.owl");

		  //getDataFormates(model);
		  getWorkflowsDatad(model);
		}
		
		
		public static void getDataFormates(Model model){
			  String queryString =
		               "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
		               "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
		               "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
		               "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
		               "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
		               "Select ?typeOfGenUse ?type (count(?type) as  ?occurances)" +
		               "WHERE "
		               + "{?genORUse rdf:type ?typeOfGenUse."
		               + "?genORUse provone:hadEntity ?dataNode. "
		               + "?dataNode senaps:dataNodeId ?nodeId."
		               + "?dataNode rdf:type ?type."
		               + "} GROUP BY ?type ?typeOfGenUse ";
		        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
		        try {
		              ResultSet results = qexec.execSelect();
		        // ResultSetFormatter.out(results);
		        ResultSetFormatter.out(System.out, results);

		            } finally {
		                qexec.close();
		             }
		}
		
		public static void getFamousDataFormatesForInput(Model model){
			  String queryString =
		               "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
		               "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
		               "PREFIX prov:<http://www.w3.org/ns/prov#>"+
		               "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
		               "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
		               "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
		               "SELECT ?type ?typeOfGenUse "
		               + "WHERE {"
		               + "{"
		               		+ "Select ?typeOfGenUse ?type (count(?type) as  ?occurances)" +
		               		"WHERE "
		               		+ "{?genORUse rdf:type prov:Usage."
		               		+ "?genORUse provone:hadEntity ?dataNode. "
		               		+ "?dataNode senaps:dataNodeId ?nodeId."
		               		+ "?dataNode rdf:type ?type."
		               		+ "} GROUP BY ?type ?typeOfGenUse }"
		               + "} Group By ?typeOfGenUse ?type "
		               + "Having Max(?occurances)";
		        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
		        try {
		              ResultSet results = qexec.execSelect();
		        // ResultSetFormatter.out(results);
		        ResultSetFormatter.out(System.out, results);

		            } finally {
		                qexec.close();
		             }
		}
		
		public static void getWorkflowsUsingSameData(Model model){
			  String queryString =
		               "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
		               "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
		               "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
		               "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
		               "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
		               "PREFIX prov:<http://www.w3.org/ns/prov#>"+
		               
		               "Select ?workflow1 ?dataNode ?plan1Id ?plan2Id " +
		               "WHERE "
		               + "{"
		               + "?exec1 prov:used ?dataNode."
		               + "?exec1 prov:qualifiedAssociation/prov:hadPlan ?plan1."
		               + "?workflow1 provone:hasSubProgram ?plan1. "
		               + "?plan1 senaps:operatorNodeId ?plan1Id."
					   + "?dataNode prov:wasGeneratedBy ?exec2."
		               + "?exec2 prov:qualifiedAssociation/prov:hadPlan ?plan2."
		               + "?workflow2 provone:hasSubProgram ?plan2. "
		               + "?plan2 senaps:operatorNodeId ?plan2Id."
//		               + "?exec1 prov:used/prov:wasGeneratedBy ?exec2."
					   + "} ORDER BY (?workflow1)";
			  
			  QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
		        try {
		              ResultSet results = qexec.execSelect();
		        // ResultSetFormatter.out(results);
		        ResultSetFormatter.out(System.out, results);

		            } finally {
		                qexec.close();
		             }
	
	}
		
		public static void getWorkflowsData(Model model){
			  String queryString =
		               "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
		               "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
		               "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
		               "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
		               "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
		               "PREFIX prov:<http://www.w3.org/ns/prov#>"+
		               
		               "Select ?workflow ?plan (count(?exec) AS ?wfExec) " +
		               "WHERE "
		               + "{"
		               + "?exec prov:qualifiedAssociation/prov:hadPlan ?plan."
		               + "?workflow provone:hasSubProgram ?plan. "
//		               + "?exec1 prov:used/prov:wasGeneratedBy ?exec2."
					   + "} GROUP BY ?plan ?workflow "
					   + "ORDER BY (?workflow) ";
			  
			  QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
		        try {
		              ResultSet results = qexec.execSelect();
		        // ResultSetFormatter.out(results);
		        ResultSetFormatter.out(System.out, results);

		            } finally {
		                qexec.close();
		            }
		}
		        
		    	public static void getWorkflowsDatad(Model model){
					  String queryString =
				               "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
				               "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
				               "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
				               "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
				               "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
				               "PREFIX prov:<http://www.w3.org/ns/prov#>"+
				               
				               "Select *  " +
				               "WHERE "
				               + "{"
				               + "?workflow provone:hasSubProgram ?opNode. "
				               + "?workflow provone:hasSubProgram ?opNode2."
							   + "}GROUP BY () "
							   + "ORDER BY (?workflow) ";
					  
					  QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
				        try {
				              ResultSet results = qexec.execSelect();
				        // ResultSetFormatter.out(results);
				        ResultSetFormatter.out(System.out, results);

				            } finally {
				                qexec.close();
				            }
			
	
	}
}
		//Resource vcard = model.getResource(johnSmithURI);}
