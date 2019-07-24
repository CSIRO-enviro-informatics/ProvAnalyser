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


public class SenapsQuery {
	public static void main(String[] args) throws Exception {
		
		  //OnDisk RDF Store
		  String directory = "C:\\Users\\but21c\\Documents\\SenapsData" ;
		  Dataset dataset = TDBFactory.createDataset(directory) ;
		  
		  //Read Ontology
		  Model model = dataset.getDefaultModel() ;
		  model.read("C:\\Users\\but21c\\Dropbox\\CSIRO_Postdoc\\ConfluxGrainsData\\SenapsOntology\\senapsLAND.owl");
		  
//		  getWorkflowsExecution(model);
//		  getAllExecutionsOfA=Program(model);
		  //getAllModels(model);
//		 findAgents(model);
		 // reproducibility(model);
	 //WorkflowExecutionProvenance(model);	
		  getWorkflowsCount(model);
	}
		


	public static void getWorkflowsCount(Model model){
		  String queryString =
	               "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
	               "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
	               "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
	               "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
	               "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
	               "Select (count(?workflow) AS ?count) " +
	               "WHERE "
	              // + "{?workflow rdf:type senaps:OperatorNodeExecution."
	               + "{?workflow rdf:type senaps:Workflow."
	               + "}";
	        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
	        try {
	              ResultSet results = qexec.execSelect();
	              // ResultSetFormatter.out(results);
	              ResultSetFormatter.out(System.out, results);

	            } finally {
	                qexec.close();
	             }
	}
	
	
	
		
		public static void getWorkflows(Model model){
			  String queryString =
		               "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
		               "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
		               "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
		               "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
		               "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
		               "Select ?workflow ?operatorNodeId ?modelId ?port ?dataNode " +
		               "WHERE "
		               + "{?workflow rdf:type senaps:Workflow;"
		               + "			senaps:workflowId ?workflowId;"
		               + "			provone:hasSubProgram ?operatorNode."
		               + "?operatorNode senaps:operatorNodeId ?operatorNodeId;"
		               + "			senaps:host ?model."
		               + "?model senaps:modelId ?modelId."
		               + "?operatorNode ?portDirection ?port. ?port rdf:type senaps:Port."
		               + "?port provone:connectsTo ?dataNode"
		               + "}";
		        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
		        try {
		              ResultSet results = qexec.execSelect();
		        // ResultSetFormatter.out(results);
		        ResultSetFormatter.out(System.out, results);

		            } finally {
		                qexec.close();
		             }
		}
		
		public static void getWorkflowsExecution(Model model){
			  String queryString =
		               "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
		               "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
		               "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
		               "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
		               "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
		               "PREFIX prov:<http://www.w3.org/ns/prov#>"+
		               
		               "Select ?workflowExec ?execId ?inport ?outport ?entity " +
		               "WHERE "
		               + "{?workflowExec rdf:type senaps:OperatorNodeExecution."
		               + "?workflowExec	senaps:executionId ?execId."
		               + "?workflowExec	prov:qualifiedUsage ?usage."
		               + "?workflowExec	prov:qualifiedGeneration ?generation."
		               + "?usage provone:hadInPort ?inport."
		               + "{{?usage provone:hadEntity ?entity.} UNION {?generation provone:hadEntity ?entity.}}"
		               + "?generation provone:hadOutPort ?outport."
		               + "FILTER (?workflowExec = <http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#5d4964d4-ac54-4720-bd0c-0b2f2559c03fforecast.output-formatter>)"
		               + "}";
		        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
		        try {
		              ResultSet results = qexec.execSelect();
		        // ResultSetFormatter.out(results);
		        ResultSetFormatter.out(System.out, results);

		            } finally {
		                qexec.close();
		             }
		}
		
		public static void getAllExecutionsOfAProgram(Model model){
			  String queryString =
		               "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
		               "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
		               "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
		               "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
		               "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
		               "PREFIX prov:<http://www.w3.org/ns/prov#>"+
		               
		               "Select ?workflowId ?association ?workflowExec " +
		               "WHERE "
		               + "{?workflowExec rdf:type senaps:OperatorNodeExecution."
		               + "?workflowExec prov:qualifiedAssociation ?association."
		               + "?association prov:hadPlan ?plan."
		               + "?workflow provone:hasSubProgram ?plan."
		               + "?workflow senaps:workflowId ?workflowId."
		               + "FILTER (?plan = <http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#forecast.output-formatter>)"
		               + "} ORDER BY (?workflowId)";
		        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
		        try {
		              ResultSet results = qexec.execSelect();
		        // ResultSetFormatter.out(results);
		        ResultSetFormatter.out(System.out, results);

		            } finally {
		                qexec.close();
		             }
		}
		
		public static void getAllModels(Model model){
			  String queryString =
		               "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
		               "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
		               "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
		               "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
		               "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
		               "PREFIX prov:<http://www.w3.org/ns/prov#>"+
		               
		               "Select ?opId ?modelId " +
		               "WHERE "
		               + "{?opNode rdf:type senaps:OperatorNode."
		               + "?opNode  senaps:host ?model."
		               + "?model senaps:modelId ?modelId."
		               + "?opNode senaps:operatorNodeId ?opId"
		               + "}";
		        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
		        try {
		              ResultSet results = qexec.execSelect();
		        // ResultSetFormatter.out(results);
		        ResultSetFormatter.out(System.out, results);

		            } finally {
		                qexec.close();
		             }
		}
		
		public static void getConnectedNodes(Model model){
			  String queryString =
		               "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
		               "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
		               "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
		               "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
		               "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
		               "PREFIX prov:<http://www.w3.org/ns/prov#>"+
		               
		               "Select DISTINCT  * " +
		               "WHERE "
		               + "{"
		               + "?genORUse rdf:type ?type."
//		               + "?genORUse provone:hadEntity/senaps:dataNodeId ?dataNodes."
					   + "?genORUse provone:hadEntity ?dataNode. ?dataNode senaps:dataNodeId \"/csiro/boorowa/temperature_surface.nc\". ?dataNode rdf:type ?type2. "
					   + "?workflowExec prov:qualifiedGeneration ?genORUse. ?workflowExec prov:qualifiedAssociation/prov:hadPlan ?plan. ?workflow provone:hasSubProgram ?plan." 
		               + "OPTIONAL {?genORUse provone:hadOutPort ?port. ?operatorNode provone:hasOutPort ?port}"
		               + "OPTIONAL {?genORUse provone:hadInPort ?port. ?operatorNode provone:hasInPort ?port}"
		               + "}";
			  
		        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
		        try {
		              ResultSet results = qexec.execSelect();
		        // ResultSetFormatter.out(results);
		        ResultSetFormatter.out(System.out, results);

		            } finally {
		                qexec.close();
		             }
		}
		
		public static void print(Model model) throws Exception
        {
            String queryString =
            		"PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
     		               "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
     		               "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
     		               "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
     		               "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
     		               "PREFIX prov:<http://www.w3.org/ns/prov#>"+
               "Select * " +
               "WHERE {senaps:da773250-4952-454e-a412-f3ed36a38418 provone:hasSubProgram ?op."
               + "?association prov:hadPlan ?op."
               + "?execution prov:qualifiedAssociation ?association. ?execution senaps:executionId ?excId."
               + "?output prov:wasGeneratedBy ?execution."
             //  + "?output prov:used ?anotherExecution."
               + "FILTER STRSTARTS(?excId, \"da773250-4952-454e-a412-f3ed36a38418\")}";
              // + "FILTER (!bound(?anotherExecution) )}";
        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
        try {
              ResultSet results = qexec.execSelect();
        // ResultSetFormatter.out(results);
        ResultSetFormatter.out(System.out, results);

            } finally {
                qexec.close();
                model.close();
             }

        }
		
		public static void prints(Model model) throws Exception
        {
            String queryString =
            		"PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
     		               "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
     		               "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
     		               "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
     		               "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
     		               "PREFIX prov:<http://www.w3.org/ns/prov#>"+
               "Select ?exec2 ?exec " +
               "WHERE {<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#8fdd4ed9-7317-4faa-9dcf-7ab18e08482e> prov:wasGeneratedBy ?exec."
               + "?exec prov:used/prov:wasGeneratedBy ?exec2.}";
        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
        try {
              ResultSet results = qexec.execSelect();
        // ResultSetFormatter.out(results);
        ResultSetFormatter.out(System.out, results);

            } finally {
                qexec.close();
                model.close();
             }

        }
		
		
		public static void printLink(Model model) throws Exception
        {
          String queryString =
          "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
          "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
          "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
          "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
          "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
          "PREFIX prov:<http://www.w3.org/ns/prov#>"+
          
          "SELECT DISTINCT ?someOp ?op ?outport ?inport " +
          "WHERE "
          + "{"
          + "<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#da773250-4952-454e-a412-f3ed36a38418> provone:hasSubProgram ?someOp. "
          + "?someOp provone:hasOutPort ?outport. "
          + "?entityGen provone:hadOutPort ?outport. "
          + "?entityGen provone:hadEntity ?entity."
          + "?entityUsed provone:hadEntity ?entity."
          + "?entityUsed provone:hadInPort ?inport. "
          + "?op provone:hasInPort ?inport."
          + "<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#da773250-4952-454e-a412-f3ed36a38418> provone:hasSubProgram ?op. "
          + "}";

        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
        try {
              ResultSet results = qexec.execSelect();
        // ResultSetFormatter.out(results);
        ResultSetFormatter.out(System.out, results);

            } finally {
                qexec.close();
                model.close();
             }

        }
		
		public static void findAgents(Model model) throws Exception
        {
          String queryString =
          "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
          "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
          "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
          "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
          "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
          "PREFIX prov:<http://www.w3.org/ns/prov#>"+
          
//          "SELECT DISTINCT ?org ?group " +
//          "WHERE "
//          + "{"
//          + "<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#da773250-4952-454e-a412-f3ed36a38418> provone:hasSubProgram ?someOp. "
//          + "?association prov:hadPlan ?someOp. "
//          + "?association	prov:agent ?org. ?org rdf:type senaps:Organisation."
//          + "?association	prov:agent ?group. ?group rdf:type senaps:Group."
//          + "}";

// "SELECT ?plan (count (DISTINCT ?agent) As ?agents) " +
//"WHERE "
//+ "{"
//+"?assoc prov:hadPlan ?plan. ?assoc prov:agent ?agent. ?agent rdf:type senaps:Group."
//+ "} GROUP BY (?plan)";

//"SELECT ?opNode ?opNode2 ?model " +
//"WHERE "
//+ "{"
//+"?opNode senaps:host ?model."
//+ "?opNode2 senaps:host ?model. "
//+ "FILTER (?opNode != ?opNode2)}";

//"SELECT ?opNode ?opNode2 ?model " +
//"WHERE "
//+ "{"
//+" ?workflowId provone:hasSubProgram ?opNode."
//+ "?opNode senaps:host ?model."
//+ "?opNode2 senaps:host ?model. "
//+ "FILTER (?opNode != ?opNode2)}";


"SELECT * " +
"WHERE "
+ "{"
+ "senaps:forecast.output-formatter provone:hasOutPort ?outport."
+ "?gen provone:hadOutPort ?outport."
+ "?gen provone:hadEntity ?entity. "
+ "}";
          
      //        graincast.apsim
          
          
        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
        try {
              ResultSet results = qexec.execSelect();
        // ResultSetFormatter.out(results);
        ResultSetFormatter.out(System.out, results);

            } finally {
                qexec.close();
                model.close();
             }

        }
		
		//find all executions that used a (specific version of) APSIM model and group them by organisations
		
		public static void tracebility(Model model) throws Exception
        {
          String queryString =
          "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
          "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
          "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
          "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
          "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
          "PREFIX prov:<http://www.w3.org/ns/prov#>"+
          
          "SELECT DISTINCT ?orgs ?workflowId " +
          "WHERE "
          + "{"
         // + "?workflowId provone:hasSubProgram ?operatorNodes."
          + "?operatorNodes senaps:host senaps:graincast.apsim."
          + "?association prov:hadPlan ?operatorNodes;"
          + "	prov:agent ?orgs. ?orgs rdf:type senaps:Organisation. "
          + "} Group By ?orgs ?workflowId ";
          
      //        graincast.apsim
          
          
        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
        try {
              ResultSet results = qexec.execSelect();
        // ResultSetFormatter.out(results);
        ResultSetFormatter.out(System.out, results);

            } finally {
                qexec.close();
                model.close();
             }

        }
		
		
		public static void reproducibility(Model model) throws Exception
        {
          String queryString =
          "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
          "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
          "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
          "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
          "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
          "PREFIX prov:<http://www.w3.org/ns/prov#>"+
          
          "SELECT DISTINCT ?model (?portId As ?variables) ?data " +
          "WHERE "
          + "{"
//         +"?asso prov:hadPlan senaps:forecast.output-formatter."
//         + "?exec prov:qualifiedAssociation ?asso."
         + "senaps:42b838a7-786c-42a0-a4b9-f7dbed9df292 (prov:wasGeneratedBy/prov:used)* ?data. "
         + "OPTIONAL {?data prov:wasGeneratedBy ?exec.}"
         + "OPTIONAL {?usage provone:hadEntity ?data. "
         			+ "?usage provone:hadInPort ?port. "
         			+ "?port senaps:portId ?portId."
         			+ "?opNode provone:hasInPort ?port."
         			+ "?opNode senaps:host ?model. }"
         + "FILTER (!bound(?exec))"
          + "} ";
          
      //        graincast.apsim
          
          
        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
        try {
              ResultSet results = qexec.execSelect();
        // ResultSetFormatter.out(results);
        ResultSetFormatter.out(System.out, results);

            } finally {
                qexec.close();
                model.close();
             }

        }
		
		
		public static void WorkflowExecutionProvenance(Model model) throws Exception
        {
          String queryString =
          "PREFIX senaps:<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#>" +
          "PREFIX provone:<http://purl.dataone.org/provone/2015/01/15/ontology#> " +
          "PREFIX rdfs:<http://www.w3.org/2000/01/rdf-schema#>"+
          "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>" +
          "PREFIX owl:<http://www.w3.org/2002/07/owl#>"+
          "PREFIX prov:<http://www.w3.org/ns/prov#>"+
          
          "CONSTRUCT {"
          + "?someOp provone:hasOutPort ?outport. "
          + "?op provone:hasInport ?inport."
          + "?someOp provone:controlledBy ?controllerURI."
          + "?controllerURI rdf:type provone:Controller."
          + "} WHERE {"
          //+ "<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#da773250-4952-454e-a412-f3ed36a38418> provone:hasSubProgram ?someOp. ?someOp senaps:operatorNodeId ?someOpId."
          + "<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#da773250-4952-454e-a412-f3ed36a38418> provone:hasSubProgram ?someOp. ?someOp senaps:operatorNodeId ?someOpId."
          + "?someOp provone:hasOutPort ?outport. ?outport senaps:portId ?outportId."
          + "?entityGen provone:hadOutPort ?outport. "
          + "?entityGen provone:hadEntity ?entity."
          + "?entityUsed provone:hadEntity ?entity."
          + "?entityUsed provone:hadInPort ?inport. ?inport senaps:portId ?inportId."
          + "?op provone:hasInPort ?inport."
          + "<http://www.csiro.au/digiscape/but21c/ontologies/senapsLAND#da773250-4952-454e-a412-f3ed36a38418> provone:hasSubProgram ?op. ?op senaps:operatorNodeId ?opId."
          + "BIND (URI(CONCAT(STR(?someOp),\".\", STR(?outportId), \"_to_\", STR(?opId),\".\",STR(?inportId))) AS ?controllerURI)}";

        System.out.println(queryString);
        QueryExecution qexec = QueryExecutionFactory.create(queryString,Syntax.syntaxSPARQL_11, model);
        try {
        	
        	
        	Model resultModel = qexec.execConstruct() ;
        	resultModel.write(System.out, "RDF/XML");
//              ResultSet results = qexec.execSelect();
//        // ResultSetFormatter.out(results);
//        ResultSetFormatter.out(System.out, results);

            } finally {
                qexec.close();
                model.close();
             }

        }
	      

	}
		//Resource vcard = model.getResource(johnSmithURI);}


