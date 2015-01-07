/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.qreator.orientdbtest3;

import com.orientechnologies.common.util.OCallable;
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
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.gremlin.groovy.Gremlin;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.util.structures.Table;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author thomas
 */
public class OrientDBTester {

    static OrientGraph graph;

    public static void main(String[] s) {

        int anzahlTags = 100;
        int anzahlDokumente = 10000;

        int minTags = 1;
        int maxTags = 5;
        boolean neuErstellen = true;

        ODatabaseDocumentTx db;
        System.out.println("Maximale WAL-Größe: " + OGlobalConfiguration.WAL_MAX_SIZE.getValue());
        OGlobalConfiguration.WAL_MAX_SIZE.setValue(1024); // größe in mb,wie groß der gesamte wal-speicher sein darf

        boolean existiertSchon = true;
        String dbPath = "plocal:/Users/thomas/dbOrient/db";

        db = new ODatabaseDocumentTx(dbPath);
        if (!db.exists()) {
            db.create();
            existiertSchon = false;
            System.out.println("Neu erstellt");
        } else {
            System.out.println("Existiert schon");
            neuErstellen = false;
        }

        String[] tags = new String[anzahlTags];
        for (int i = 0; i < anzahlTags; i++) {
            String tag = "";
            for (int j = 5; j <= 10; j++) {
                tag += (char) ((int) (65 + Math.random() * 24));
            }
            //System.out.println(tag);
            tags[i] = tag;
        }

        //OrientGraph graph = new OrientGraph("plocal:/Users/thomas/dbOrient/db");    
        try {
            OrientGraphFactory factory = new OrientGraphFactory(dbPath).setupPool(1, 10);
            graph = factory.getTx();
            if (!existiertSchon) {
                // http://www.kwoxer.de/2014/11/12/daten-import-via-java-orientdb-real-beispiel-tutorial/
                graph.executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
                    public Object call(OrientBaseGraph iArgument) {
                        graph.createVertexType("tags").setClusterSelection("default");
                        graph.createVertexType("tag").setClusterSelection("default");
                        graph.createVertexType("doc").setClusterSelection("default");
                        graph.createEdgeType("hatTag").setClusterSelection("default");
                        graph.createEdgeType("hat").setClusterSelection("default");
                        return null;
                    }
                });
            }
        } catch (Exception e) {

        }
        //

        if (neuErstellen) { // nur neu erstellen, wenn es nicht existiert
            try {
                Vertex tagVertex = graph.addVertex("class:tags");

                for (int i = 0; i < tags.length; i++) {
                    Vertex tag = graph.addVertex("class:tag");
                    tag.setProperty("tagname", tags[i]);
                    tag.setProperty("anzahl", 0L);
                    tagVertex.addEdge("hat", tag);
                }
                graph.commit();

                // erstellen von edges
            } catch (Exception e) {
                graph.rollback();
            }

            Iterator<Vertex> it = graph.getVerticesOfClass("tag").iterator();
            ArrayList<Vertex> al = new ArrayList<Vertex>();
            while (it.hasNext()) {
                Vertex v = it.next();

                al.add(v);

            }

            for (int i = 0; i < anzahlDokumente; i++) {
                String tag = "";
                for (int j = 5; j <= 10; j++) {
                    tag += (char) ((int) (65 + Math.random() * 24));
                }
                if (i % 1000 == 0) {
                    System.out.println("" + i);
                }
                tag = "D" + i + "-" + tag;
                //System.out.println(tag);
                try {

                    Vertex doc = graph.addVertex("class:doc");
                    doc.setProperty("docname", tag);

                    // System.out.print("Dokument "+tag+" mit id "+doc.getId().toString()+": ");
                    int anzahl = (int) (Math.random() * (maxTags - minTags + 1) + minTags);

                    for (int j = 0; j < anzahl; j++) {
                        int zz = (int) (Math.random() * anzahlTags);
                        Vertex v = al.get(zz);
                        doc.addEdge("hatTag", v);
                        v.setProperty("anzahl", (Long) (v.getProperty("anzahl")) + 1);
                    //graph.addEdge("class:hatTag", doc, al.get(zz),null);

                        //  System.out.print(" "+al.get(zz).getProperty("tagname"));
                    }

                    // System.out.println("");
                    graph.commit();
                } catch (Exception e) {
                    graph.rollback();
                }
            }
        }
            //Iterator<Vertex> it2 = graph.getVertices("tagname", tags[3]).iterator();
        //System.out.println("Hole Vertex 3 mit Inhalt " + tags[3] + ":" + it2.next().getProperty("tagname"));
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
        boolean weiter = true;
        HashMap m = new HashMap();
        Iterator<Vertex> it9 = graph.getVerticesOfClass("tags").iterator(); //haupttag holen   
        long letzteZeit=0;
        int letzteTreffer=0;

        long zeit1 = (new Date()).getTime();
        ArrayList<Vertex> svT = new ArrayList<>();
        GremlinPipeline pipe2 = new GremlinPipeline();
        Vertex startVertex = it9.next();
        pipe2.start(startVertex).out("hat");//.in("hatTag").dedup(); // mit altem tag starten
        //pipe2.out("hatTag").groupCount(m).dedup();

        Iterator<Vertex> it10 = pipe2.iterator();
        while (it10.hasNext()) {
            // legt die hashmap an
            Vertex v = it10.next();
            m.put(v, v.getProperty("anzahl"));
            //long anzahlEcken=(new GremlinPipeline(v)).inE("hatTag").count();
            //m.put(v, graph.getEdges);
            svT.add(v);

        }

        /* pipe2 = new GremlinPipeline();
         pipe2.start(startVertex).out("hat").inE("hatTag").count();
         it10 = pipe2.iterator();
         while (it10.hasNext()) {
         // legt die hashmap an
         //svT.add(it10.next());
         System.out.println(it10.next());
         }*/
        long zeit2 = (new Date()).getTime();
        System.out.println("Dauer für Tagsuche: " + (zeit2 - zeit1) + " ms");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

        ArrayList<Vertex> auswahl = new ArrayList<>();
        ArrayList<Vertex> docs=new ArrayList<>();
        HashMap  docmap=new HashMap<>();
        boolean neustart = false;
        Table t=new Table();
        //Pipe p=Gremlin.compile("_().table(t){it.name}");
        while (weiter) {
            // liste aller sinnvollen tags ausgeben
            System.out.println("Liste der Tags: ");
            for (int i = 0; i < svT.size(); i++) {
                Vertex v=svT.get(i);
                if (m.get(v) != null && (long) (m.get(v)) > 0) {
                    
                    System.out.print((i + 1) + ": " + v.getProperty("tagname"));
                    if (m.get(v) != null) {
                        System.out.print(", Anzahl: " + m.get(v));
                        
                    } 
                        System.out.println("");
                    
                } /*else {
                    System.out.println((i + 1) + ": " + svT.get(i).getProperty("tagname"));
                }*/
            }
            if (t.getRowCount()>0){
                System.out.println("Dokumente mit diesen Tags:");
            for (int i=0;i<t.getRowCount();i++){
                
                Vertex v=(Vertex)(t.get(i,0));
                System.out.print(i+": "+v.getProperty("docname")+": ");
                GremlinPipeline gp=new GremlinPipeline();
                gp.start(v).out("hatTag").dedup(); // alle tags holen
                Iterator it12=gp.iterator();
                int z=0;
                while (it12.hasNext()){
                    System.out.print(" "+((Vertex)(it12.next())).getProperty("tagname"));
                    z++;
                }
                if (z==auswahl.size()){ // alle tags erfüllt
                    System.out.print(" ****");
                }
                System.out.println("");
            }
            }
            System.out.print("Gewählte Tags: ");
            for (int i = 0; i < auswahl.size(); i++) {
                System.out.print("-" + (i + 1) + ": " + auswahl.get(i).getProperty("tagname") + "  ");
            }
            System.out.println("");
            if (letzteZeit>0){
                System.out.println("Treffer: "+letzteTreffer+", Dauer: "+letzteZeit+" ms");
            }
            System.out.print("Bitte Nummer wählen ('q' zum beenden): ");
            int treffer = 0;
            try {
                String eingabe = br.readLine();
                if (eingabe.equals("q")) {
                    weiter = false;
                } else {
                    int i = Integer.parseInt(eingabe);
                    zeit1 = (new Date()).getTime();
                    
                    GremlinPipeline pipe = new GremlinPipeline();
                    if (i > 0) {
                        i--;
                        //auswahl.add(graph.getVertex(svT.get(i).getId()));
                        //pipe.start(graph.getVertex(svT.get(i).getId())).in("hatTag").dedup();
                        auswahl.add(svT.get(i));
                        pipe.start(svT.get(i)).in("hatTag").dedup();
                        svT.remove(i);
                    } else {
                        i = -i;
                        i--;

                        Vertex v = auswahl.get(i);
                        auswahl.remove(i);
                        svT.add(v);
                        if (auswahl.size() > 0) {
                            pipe.start(auswahl.get(0)).in("hatTag").dedup(); // mit altem tag starten
                        } else {
                            //Iterator<Vertex> it8 = graph.getVerticesOfClass("tags").iterator(); //haupttag holen                       
                            pipe.start(startVertex).out("hat"); // mit altem tag starten   
                            neustart = true;
                        }

                    }
                    
                    m = new HashMap();
                    t=new Table();
                   /* docs=new ArrayList<>();
                    docmap=new HashMap();*/
             
                    for (int j = 0; j < auswahl.size(); j++) { // überprüft alle tags (entspricht "und")
                        pipe.as("x").out("hatTag").has("tagname", auswahl.get(j).getProperty("tagname")).back("x");
                    }
                    if (!neustart) {
                        pipe.table(t).out("hatTag").groupCount(m).dedup();

                    }
                    treffer = 0;
                    ArrayList<Vertex> temp = new ArrayList<>();
                    Iterator<Vertex> it6 = pipe.iterator();

                    while (it6.hasNext()) {
                        treffer++;
                        
                        Vertex v = it6.next();
                     /*   if (docs.size()>0){
                        docmap.put(v, docs);
                        docs=new ArrayList<>();
                        }*/
                        
                        if (neustart) {
                            m.put(v, v.getProperty("anzahl"));
                        }

                    }
                    neustart = false;

                    //svT.clear();
                 /*   for (int k = 0; k < temp.size(); k++) {
                     svT.add(temp.get(k));
                     }*/
                    // map mit anzahlen ausgeben
                   /* Iterator<Vertex> i7 = m.keySet().iterator();
                     while (i7.hasNext()) {
                     Vertex v = i7.next();
                     System.out.print("Tag: " + v.getProperty("tagname"));
                     System.out.println(", Anzahl: " + m.get(v));

                     }*/
                    zeit2 = (new Date()).getTime();
                    System.out.println("Treffer: " + treffer + ", Dauer: " + (zeit2 - zeit1) + " ms");
                    letzteZeit=(zeit2-zeit1);
                    letzteTreffer=treffer;
                }

            } catch (Exception e) {
                e.printStackTrace();
                weiter = false;
            }

        }

    }

}
