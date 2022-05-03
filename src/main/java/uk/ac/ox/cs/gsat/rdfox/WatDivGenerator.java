package uk.ac.ox.cs.gsat.rdfox;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;

import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.Predicate;
import uk.ac.ox.cs.pdq.fol.TGD;
import uk.ac.ox.cs.pdq.fol.Variable;

public class WatDivGenerator {

    private final static int CONCEPT_FACTOR = 250;
    private final static int ROLE_FACTOR = 10;
    // private final static int SCALE_FACTOR = 10;
    private final static int DOMAIN_RANGE_LIMIT = 10;

    // body roles and concepts
    Set<Predicate> roles = new HashSet<>();

    Set<Predicate> concepts = new HashSet<>();
    Map<Predicate, String> namespacedConcept = new HashMap<>();

    Map<Predicate, Set<Predicate>> roleDomainConcepts = new HashMap<>();
    Map<Predicate, Set<Predicate>> roleRangeConcepts = new HashMap<>();
    Map<Predicate, Set<Predicate>> conceptIntersections = new HashMap<>();

    int prefixCount = 0;
    Map<String, String> prefixes = new HashMap<String, String>();
    Map<String, String> prefixesInv = new HashMap<String, String>();

    private Set<Predicate> ranges = new HashSet<>();

    private Set<Predicate> domains = new HashSet<>();

    private final Collection<TGD> tgds;
    private final String dataPath;
    private final String watdivPath;
    private final int scaleFactor;

    public WatDivGenerator(Collection<TGD> tgds, String dataPath, int scaleFactor) {
        this.tgds = tgds;
        this.dataPath = dataPath;
        this.watdivPath = getWatdivPath(dataPath);
        this.scaleFactor = scaleFactor;
    }

    public int generate() throws IOException {

        for (TGD tgd : tgds) {

            Map<Variable, Set<Predicate>> rolePerDomainVariable = new HashMap<>();
            Map<Variable, Set<Predicate>> rolePerRangeVariable = new HashMap<>();
            Map<Variable, Set<Predicate>> conceptPerVariable = new HashMap<>();

            for (Atom atom : tgd.getBodyAtoms()) {
                Predicate predicate = atom.getPredicate();
                if (predicate.getArity() == 2) {
                    if (atom.getTerm(0).isVariable()) {
                        addOrCreate(rolePerDomainVariable, (Variable) atom.getTerm(0), predicate);
                    }
                    if (atom.getTerm(1).isVariable()) {
                        addOrCreate(rolePerRangeVariable, (Variable) atom.getTerm(1), predicate);
                    }
                    roles.add(predicate);
                } else {
                    if (atom.getTerm(0).isVariable()) {
                        addOrCreate(conceptPerVariable, (Variable) atom.getTerm(0), predicate);
                    }
                    concepts.add(predicate);
                }
            }

            for (Variable v : rolePerDomainVariable.keySet()) {
                for (Predicate role : rolePerDomainVariable.get(v)) {
                    for (Predicate concept : conceptPerVariable.getOrDefault(v, Set.of())) {
                        domains.add(concept);
                        addOrCreate(roleDomainConcepts, role, concept);
                    }
                }
            }

            for (Variable v : rolePerRangeVariable.keySet()) {
                for (Predicate role : rolePerRangeVariable.get(v)) {
                    for (Predicate concept : conceptPerVariable.getOrDefault(v, Set.of())) {
                        ranges.add(concept);
                        addOrCreate(roleRangeConcepts, role, concept);
                    }
                }
            }

            for (Set<Predicate> cluster: conceptPerVariable.values())
                for (Predicate c1: cluster)
                    for (Predicate c2 : cluster)
                        if (!c1.equals(c2))
                            addOrCreate(conceptIntersections, c1, c2);

        }

        for (Predicate p : concepts) {
            namespacedConcept.put(p, getNamespaced(p));
            // System.out.println(p);
        }

        for (Predicate p : roleDomainConcepts.keySet()) {
            System.out.println("Role " + p + " with domains " + roleDomainConcepts.get(p));
        }

        for (Predicate p : roleRangeConcepts.keySet()) {
            System.out.println("Role " + p + " with ranges " + roleRangeConcepts.get(p));
        }

        // write the watdiv schema to a file
        File file = new File(watdivPath);
        file.delete();
        file.createNewFile();

        BufferedWriter writer = new BufferedWriter(new FileWriter(watdivPath));

        writeNamespaces(writer);
        writeConcepts(writer);
        writeRoles(writer);

        writer.close();

        return runWatDiv();
    }

    private int runWatDiv() throws IOException {

        ProcessBuilder pb = new ProcessBuilder("./watdiv","-d", watdivPath, ((Integer) scaleFactor).toString());
        Process process = pb.start();

        BufferedReader out = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line;

        File file = new File(dataPath);
        file.delete();
        file.createNewFile();

        BufferedWriter writer = new BufferedWriter(new FileWriter(this.dataPath));

        int count = 0;
        while((line = out.readLine()) != null) {
            count++;
            // remove the suffix "0" from the concept name
            for (Predicate concept : concepts) {
                line = line.replace(concept.toString() + "0>", concept.toString() + ">");
            }

            writer.write(line);
            writer.write("\n");
        }

        writer.close();
        return count;
    }

    private String getNamespaced(Predicate predicate) {
        String name = predicate.toString();
        int indexOfSlash = name.lastIndexOf('#');
        String namespaceURL = name.substring(0, indexOfSlash +1);
        String prefix; 
        String localPath = name.substring(indexOfSlash +1);

        if (prefixesInv.containsKey(namespaceURL)){
            prefix = prefixesInv.get(namespaceURL);
        } else {
            prefix = getFreshPrefix();
            prefixesInv.put(namespaceURL, prefix);
            prefixes.put(prefix, namespaceURL);
        }

        return prefix + ":" + localPath;
    }

    private String getFreshPrefix() {
        return "local" + prefixCount++;
    }

    public void writeRoles(BufferedWriter writer) throws IOException {

        for (Predicate role : roles) {
            Collection<Predicate> domains = getRoleDomain(role);
            Collection<Predicate> ranges = getRoleRange(role);
            double prob = ROLE_FACTOR / (double) ((domains.size() + ranges.size()));
            for (Predicate domain : domains) {
                for (Predicate range : ranges) {
                    String roleName = getNamespaced(role);
                    String domainName = getNamespaced(domain);
                    String rangeName = getNamespaced (range);

                    writer.write("#association	" + domainName + "Entity " + roleName + " " + rangeName
                            + "Entity	2 1		"+ prob +"	UNIFORM\n\n");
                }
            }
        }
    }
    
    private Collection<Predicate> getRoleDomain(Predicate role) {
        if (roleDomainConcepts.containsKey(role))
            return roleDomainConcepts.get(role);
        else {
            List<Predicate> d = new ArrayList<>(concepts);
            Collections.shuffle(d);
            return d.subList(0, DOMAIN_RANGE_LIMIT);
        }
    }

    private Collection<Predicate> getRoleRange(Predicate role) {
        if (roleRangeConcepts.containsKey(role))
            return roleRangeConcepts.get(role);
        else {
            List<Predicate> d = new ArrayList<>(concepts);
            Collections.shuffle(d);
            return d.subList(0, DOMAIN_RANGE_LIMIT);
        }
    }

    public void writeConcepts(BufferedWriter writer) throws IOException {

        int normalizedConceptFactor = Math.max(1, (CONCEPT_FACTOR / concepts.size()));
        
        for (Predicate concept : concepts) {
            String conceptName = namespacedConcept.get(concept);
            writer.write("// Meta concept \n");
            writer.write("<type*> ");
            writer.write(conceptName);
            writer.write(" 1\n");
            writer.write("</type>\n\n");

            writer.write("// Concept Entities \n");
            writer.write("<type> ");
            writer.write(conceptName + "Entity ");
            writer.write(normalizedConceptFactor + "\n");
            writer.write("</type>\n\n"); 
                     
                    
            writer.write("#association	" + conceptName+ "Entity 	rdf:type 		" + conceptName + "	2 1		1.0	UNIFORM\n\n");

            Set<Predicate> inter = conceptIntersections.getOrDefault(concept, Set.of());
            double prob = 1.0 / inter.size();
            for (Predicate other : inter) {
                String otherName = namespacedConcept.get(other);
                writer.write(
                        "#association	" + conceptName + "Entity 	rdf:type 		" + otherName +"	2 1		"+ prob +"	UNIFORM\n\n");
            }
        }
    }

    public void writeNamespaces(BufferedWriter writer) throws IOException {
        writer.write("#namespace	rdf=http://www.w3.org/1999/02/22-rdf-syntax-ns#\n");

        for (Entry<String, String> entry : prefixes.entrySet()) {
            writer.write("#namespace "+ entry.getKey()+"="+entry.getValue()+ "\n");
        }

        writer.write("\n");
    }

    public static <K, V> void addOrCreate(Map<K, Set<V>> map, K key, V value) {
        if (map.containsKey(key)) {
            map.get(key).add(value);
        } else {
            HashSet<V> set = new HashSet<>();
            set.add(value);
            map.put(key, set);
        }
    }

    public static String getWatdivPath(String dataPath) {
        return Paths.get(dataPath).getParent().resolve(FilenameUtils.getBaseName(dataPath) + "-watdiv.txt").toString();
    }

}
