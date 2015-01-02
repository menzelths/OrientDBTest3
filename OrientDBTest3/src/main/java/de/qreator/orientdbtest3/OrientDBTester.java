/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.qreator.orientdbtest3;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

/**
 *
 * @author thomas
 */
public class OrientDBTester {

    public static void main(String[] s) {

        int anzahlTags = 1000;
        int anzahlDokumente = 1000000;
        int minTags = 1;
        int maxTags = 10;
        OrientGraph graph;

        String[] tags = new String[anzahlTags];
        for (int i = 0; i < anzahlTags; i++) {
            String tag = "";
            for (int j = 5; j <= 10; j++) {
                tag += (char) ((int) (65 + Math.random() * 24));
            }
            System.out.println(tag);
            tags[i] = tag;
        }

    //OrientGraph graph = new OrientGraph("plocal:/Users/thomas/dbOrient/db");    
        try {
            OrientGraphFactory factory = new OrientGraphFactory("plocal:/Users/thomas/dbOrient/db").setupPool(1, 10);
            graph = factory.getTx();
            System.out.println("Existiert schon");
        } catch (Exception e) {
            graph = new OrientGraph("plocal:/Users/thomas/dbOrient/db");
            System.out.println("Neu erstellt");
        }
        try {
            for (int i = 0; i < tags.length; i++) {
                Vertex tag = graph.addVertex(null);
                tag.setProperty("tagname", tags[i]);
                graph.commit();
            }
            // erstellen von edges

            Iterator<Vertex> it = graph.getVertices().iterator();
            ArrayList<Vertex> al = new ArrayList<Vertex>();
            while (it.hasNext()) {
                Vertex v = it.next();
                if (v.getProperty("tagname") != null) { // tag-vertex
                    al.add(v);
                }

            }

            for (int i = 0; i < anzahlDokumente; i++) {
                String tag = "";
                for (int j = 5; j <= 10; j++) {
                    tag += (char) ((int) (65 + Math.random() * 24));
                }
                
                tag = "D" + i + "-" + tag;
                //System.out.println(tag);
                Vertex doc = graph.addVertex(null);
                doc.setProperty("docname", tag);
                graph.commit();
                //System.out.print("Dokument "+tag+" mit id "+doc.getId().toString()+": ");
                int anzahl = (int) (Math.random() * (maxTags - minTags + 1) + minTags);

                for (int j = 0; j < anzahl; j++) {
                    int zz = (int) (Math.random() * anzahlTags);
                    
                    Edge e=graph.addEdge(null, doc, al.get(zz), "hatTag");
                   // System.out.print(" "+al.get(zz).getProperty("tagname"));

                }
               //System.out.println("");
            }
       
            graph.commit();
            //Iterator<Vertex> it2 = graph.getVertices("tagname", tags[3]).iterator();

            //System.out.println("Hole Vertex 3 mit Inhalt " + tags[3] + ":" + it2.next().getProperty("tagname"));

            Iterator<Vertex> it3 = graph.getVertices().iterator();
            while (it3.hasNext()) {
                Vertex v = it3.next();
                //System.out.println(v.getProperty("docname") + " " + v.getId());
            }
            
            Iterator<Edge> it4=graph.getEdges().iterator();
            int anzahlEcken=0;
            while (it4.hasNext()) {
                anzahlEcken++;
                Edge e = it4.next();
               // System.out.println("Ecke "+anzahlEcken+" von "+e.getVertex(Direction.OUT).getProperty("docname") + " nach " + e.getVertex(Direction.IN).getProperty("tagname"));
            }
            System.out.println("Anzahl Ecken: "+anzahlEcken);
            /* GremlinPipeline pipe=new GremlinPipeline();
             pipe.start(graph.getVertex("#9:3")).property("tagname");
             int treffer=0;
             while (pipe.hasNext()){
             treffer++;
            
             System.out.println("Treffer "+treffer+": "+pipe.next());
             }*/
            /* Vertex luca = graph.addVertex(null); // 1st OPERATION: IMPLICITLY BEGIN A TRANSACTION
             luca.setProperty("name", "Thomas");
             Vertex marko = graph.addVertex(null);
             marko.setProperty("name", "Patrick");
             Edge lucaKnowsMarko = graph.addEdge(null, luca, marko, "knows");
             graph.commit();*/
            
            
             // tests zur geschwindigkeit
             boolean weiter=true;
                long zeit1=(new Date()).getTime();
                Iterator<Vertex> it5=graph.getVertices().iterator();
                ArrayList<Vertex> alleTags=new ArrayList<Vertex>();
                ArrayList<Vertex> svT=new ArrayList<Vertex>();
                while(it5.hasNext()){
                    Vertex v=it5.next();
                    if (v.getProperty("tagname")!=null){
                    alleTags.add(v);
                    svT.add(v);
                    }
                }
                long zeit2=(new Date()).getTime();
                System.out.println("Dauer für Tagsuche: "+(zeit2-zeit1)+" ms");
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
 
                ArrayList<Integer> auswahl=new ArrayList<Integer>();
        while (weiter){
            // liste aller sinnvollen tags ausgeben
            System.out.println("Liste der Tags: ");
            for (int i=0;i<svT.size();i++){
                System.out.println(i+": "+svT.get(i).getProperty("tagname"));
            }
            System.out.print("Bitte Nummer wählen: ");
            int treffer=0;
            try{
                String eingabe=br.readLine();
                int i=Integer.parseInt(eingabe);
                zeit1=(new Date()).getTime();
                
                if (i>=0){
                    auswahl.add(i);
                    GremlinPipeline pipe=new GremlinPipeline();
                    
             pipe.start(graph.getVertex(svT.get(i).getId())).in("hatTag").dedup().out("hatTag").dedup();
             treffer=0;
             ArrayList<Vertex> temp=new ArrayList<Vertex>();
             Iterator<Vertex> it6=pipe.iterator();
             while (it6.hasNext()){
             treffer++;
                
                Vertex v=it6.next();
                if (svT.contains(v)){
                    temp.add(v);
                }
                 System.out.println(""+v.getProperty("tagname"));
             //System.out.println("Treffer "+treffer+": "+pipe.next().toString());
             }
             svT.clear();
             for (int k=0;k<temp.size();k++){
                 svT.add(temp.get(k));
             }
             zeit2=(new Date()).getTime();
                System.out.println("Treffer: "+treffer+", Dauer: "+(zeit2-zeit1)+" ms");
                } else {
                    weiter=false;
                }
                
                
            }catch(Exception e){
            e.printStackTrace();
            weiter=false;}
            
        }

            
            
        } catch (Exception e) {

            graph.rollback();
            e.printStackTrace();
        }
        
        
        
       
    }
    
    

}
