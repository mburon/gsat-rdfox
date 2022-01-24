package uk.ac.ox.cs.gsat.rdfox;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FilenameUtils;

import tech.oxfordsemantic.jrdfox.Prefixes;
import tech.oxfordsemantic.jrdfox.exceptions.JRDFoxException;
import tech.oxfordsemantic.jrdfox.logic.expression.IRI;
import tech.oxfordsemantic.jrdfox.logic.sparql.pattern.TriplePattern;
import uk.ac.ox.cs.gsat.DLGPIO;
import uk.ac.ox.cs.gsat.rdfox.statistics.StatisticsCollector;
import uk.ac.ox.cs.gsat.rdfox.statistics.StatisticsLogger;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.Dependency;
import uk.ac.ox.cs.pdq.fol.Predicate;
import uk.ac.ox.cs.pdq.fol.TGD;

public class App {

    private final static String INPUT_REGEX = ".*\\.rul";
    private final static String STATS_FILENAME = "mat-stats.csv";
    
    public static void main(String[] args) throws Exception {
        StatisticsCollector statsCollector = new StatisticsCollector();
        StatisticsLogger statsLogger;

        
        if (args.length == 1) {
            // with the only argument can be either the path to a TGDs file or a directory to browse
            
            if (new File(args[0]).isDirectory()) {
                String inputDirectory = getAbsolutePath(args[0]);

                statsLogger = getStatisticsLogger(statsCollector, inputDirectory);
                
                List<String> tgdsPaths = Files
                        .find(Paths.get(inputDirectory), 999,
                                (p, bfa) -> bfa.isRegularFile() && p.getFileName().toString().matches(INPUT_REGEX))
                    .map(p -> p.toString())
                    .collect(Collectors.toList());

                Collections.sort(tgdsPaths);
                
                statsLogger.printHeader();
                for (String tgdsPath : tgdsPaths) {
                    runFromTGDFile(tgdsPath, statsCollector);
                    statsLogger.printRow(getRowName(tgdsPath));
                }
                
            } else {
                String tgdsPath = getAbsolutePath(args[0]);

                statsLogger = getStatisticsLogger(statsCollector, null);
                
                runFromTGDFile(tgdsPath, statsCollector);
                statsLogger.printHeader();
                statsLogger.printRow(getRowName(tgdsPath));
            }

        } else if(args.length == 2 || args.length == 3) {
            // with 2 or 3 arguments
            // the first argument is a data file
            String dataPath = getAbsolutePath(args[0]);
            // the second argument is a TGDs file
            String tgdsPath = getAbsolutePath(args[1]);
            // the materialization file is given or induced
            String outputPath;

            if (args.length == 2 ) {
                outputPath = getMaterializationPath(tgdsPath);
            } else {
                outputPath = getAbsolutePath(args[2]);
            }

            statsLogger = getStatisticsLogger(statsCollector, null);
            String rowName = getRowName(tgdsPath);
            statsCollector.start(rowName);
            run(dataPath, parseDLGP(tgdsPath), outputPath, statsCollector, rowName);
            statsLogger.printHeader();
            statsLogger.printRow(getRowName(tgdsPath));

        } else {
            System.out.println("the arguments are: <tgds file or directory containing tgds files>");
            System.out.println("the arguments are: <input data file> <rule file> [<output file>]");
            System.out.println(
                    "if <input data file> is not given then the input data will be generated automatically into a NTriple file from the TGDs");
            return;
        }

        statsLogger.close();
    }

    public static void runFromTGDFile(String tgdsPath, StatisticsCollector statsCollector) throws IOException, JRDFoxException {

        Collection<TGD> fullTGDs;
        try {
            fullTGDs = parseDLGP(tgdsPath);
        } catch (Exception e) {
            System.out.println(String.format("Failed to parse %s with:\n%s", tgdsPath, e.getMessage()));
            return;
        }

        String rowName = getRowName(tgdsPath);
        String inputPath = getInputPath(tgdsPath);
        String outputPath = getMaterializationPath(tgdsPath);
        statsCollector.start(rowName);
        int inputSize = generateNTriplesFromTGDs(fullTGDs, inputPath);
        statsCollector.tick(rowName, MaterializationStatColumns.MAT_GEN_TIME);
        statsCollector.put(rowName, MaterializationStatColumns.MAT_GEN_SIZE, inputSize);

        run(inputPath, fullTGDs, outputPath, statsCollector, rowName);
    }

    public static void run(String inputPath, Collection<TGD> fullTGDs, String materializationPath,
            StatisticsCollector statsCollector, String rowName) throws FileNotFoundException, JRDFoxException {
        RDFoxMaterializer materializer = new RDFoxMaterializer();

        statsCollector.put(rowName, MaterializationStatColumns.MAT_FTGD_NB, fullTGDs.size());
        statsCollector.resume(rowName);
        long materizationSize = materializer.materialize(inputPath, fullTGDs, materializationPath);
        statsCollector.tick(rowName, MaterializationStatColumns.MAT_TIME);
        statsCollector.put(rowName, MaterializationStatColumns.MAT_SIZE, materizationSize);
    }

    public static Collection<TGD> parseDLGP(String tgdsPath) throws Exception  {
        System.out.println(String.format("Parsing %s ...", tgdsPath));
        DLGPIO parser = new DLGPIO(tgdsPath, false);

        Collection<TGD> fullTGDs = new ArrayList<>();
        for (Dependency dependency : parser.getRules()) {
            if (dependency instanceof TGD && !((TGD) dependency).isExistential()) {
                fullTGDs.add((TGD) dependency);
            }
        }
        return fullTGDs;
    }

    public static String getRowName(String tgdPath) {
        return FilenameUtils.getBaseName(tgdPath);
    }

    public static String getInputPath(String tgdPath) {
        return Paths.get(tgdPath).getParent().resolve(FilenameUtils.getBaseName(tgdPath) + "-input.nt").toString();
    }

    public static String getMaterializationPath(String tgdPath) {
        return Paths.get(tgdPath).getParent().resolve(FilenameUtils.getBaseName(tgdPath) + "-mat.nt").toString();
    }

    public static StatisticsLogger getStatisticsLogger(StatisticsCollector statsCollector, String inputDirectory) throws FileNotFoundException {

        StatisticsLogger statsLogger;
        if (inputDirectory != null) {
            String statsFilePath = Paths.get(inputDirectory).resolve(STATS_FILENAME).toString();
            new File(statsFilePath).delete();
            PrintStream statsStream = new PrintStream(new FileOutputStream(statsFilePath, true));
            statsLogger = new StatisticsLogger(statsStream, statsCollector);
        } else {
            statsLogger = new StatisticsLogger(System.out, statsCollector);
        }
        statsLogger.setSortedHeader(Arrays.asList(MaterializationStatColumns.values()));

        return statsLogger;
    }

    /**
     * write a Ntriple files containing triples of the following forms 
     * c rdf:type P or c P c whether the predicate P is unary or binary
     * with c being a fixed IRI 
     * The used predicates P are those in the body of the tgd but in their head 
     */
    public static int generateNTriplesFromTGDs(Collection<TGD> tgds, String fileName) throws IOException {

        Set<Predicate> bodyPredicates = new HashSet<>();
        Set<Predicate> headPredicates = new HashSet<>();
        for (TGD tgd : tgds) {
            for (Atom atom : tgd.getBodyAtoms()) {
                bodyPredicates.add(atom.getPredicate());
            }

            for (Atom atom : tgd.getHeadAtoms()) {
                headPredicates.add(atom.getPredicate());
            }
        }

        // we consider only the predicates that appear in TGD bodies but in heads
        bodyPredicates.removeAll(headPredicates);

        Collection<TriplePattern> triples = new ArrayList<>();
        IRI constant = IRI.create("http://example.com/c");
        for (Predicate predicate : bodyPredicates) {
            TriplePattern triple;
            if (predicate.getArity() == 1) {
                triple = TriplePattern.create(constant, IRI.RDF_TYPE, RDFoxFactory.predicateAsIRI(predicate));
            } else if (predicate.getArity() == 2) {
                triple = TriplePattern.create(constant, RDFoxFactory.predicateAsIRI(predicate), constant);
            } else {
                String message = String.format("The predicate %s is neither unary nor binary", predicate);
                throw new IllegalStateException(message);
            }
            triples.add(triple);
        }

        // write the triples to a file
        File file = new File(fileName);
        file.delete();
        file.createNewFile();

        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));

        for (TriplePattern triple : triples) {
            writer.write(triple.toString(Prefixes.s_emptyPrefixes) + " .");
            writer.write("\n");
        }

        writer.close();
        return triples.size();
    }

    public static String getAbsolutePath(String relativePath) {
        return new File(relativePath).getAbsolutePath();
    }

}
