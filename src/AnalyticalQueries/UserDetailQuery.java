package AnalyticalQueries;
import java.util.HashMap;

import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Resource;



public class UserDetailQuery {
	
	public static HashMap<String, String> getWorkflowsCount(Model model, String workflow){
		  HashMap<String, String> res = new HashMap<String, String>();
		
		  String queryString =
	               "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
	               "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
	               "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
	               "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
	               "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
		           "PREFIX prov:<http://www.w3.org/ns/prov#>"+
	               
		           "Select ?association ?execution " +
	               "WHERE "
	               //+ "{?workflow rdf:type senaps:Workflow."
	               + "{senaps:"+workflow+" provone:hasSubProgram ?opNode."
	               + "?association prov:hadPlan ?opNode."
	               + "?execution prov:qualifiedAssociation ?association. ?execution senaps:executionId ?excId."
	               + "FILTER STRSTARTS(?excId, \""+workflow+"\")"
	               + "}";
		  
		   //System.out.println(queryString);
	        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
	        try {
	              ResultSet results = qexec.execSelect();
	              while (results.hasNext()){
	            	  QuerySolution qs = results.next();
	            	  Resource association = qs.getResource("?association");
	            	  Resource execution = qs.getResource("?execution");
	            	  res.put(execution.toString(),association.toString() );
	              }

	            } finally {
	                qexec.close();
	             }
	        return res;
	}
	
		

	      

	}
		//Resource vcard = model.getResource(johnSmithURI);}
