package SenapsWorkflowBeans;

import java.util.ArrayList;

public class AllLink {
	public ArrayList<Link> links = new ArrayList<Link>();
	
	public void addElementInList(ArrayList<Link> link){
		this.links = link;
	}
	
	public ArrayList<Link> getLinks() {
		return this.links;
	}
}
