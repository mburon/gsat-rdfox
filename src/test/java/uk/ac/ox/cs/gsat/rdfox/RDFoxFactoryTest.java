package uk.ac.ox.cs.gsat.rdfox;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

import tech.oxfordsemantic.jrdfox.Prefixes;
import tech.oxfordsemantic.jrdfox.exceptions.ParsingException;
import tech.oxfordsemantic.jrdfox.formats.DatalogParser;
import tech.oxfordsemantic.jrdfox.logic.datalog.Rule;
import uk.ac.ox.cs.pdq.fol.Atom;
import uk.ac.ox.cs.pdq.fol.Predicate;
import uk.ac.ox.cs.pdq.fol.TGD;
import uk.ac.ox.cs.pdq.fol.Variable;

public class RDFoxFactoryTest {

    @Test
    public void testRuleAboutParent() throws ParsingException, IOException {
        DatalogParser parser = new DatalogParser(Prefixes.s_emptyPrefixes);
        parser.bind(getStreamFromResourceFile("rule-about-parent.txt"));
        Rule parsedRule = parser.parseRule();

        Variable x = Variable.create("x");
        Variable y = Variable.create("y");
        Predicate hasParent = Predicate.create("https://oxfordsemantic.tech/RDFox/getting-started/hasParent", 2);
        Predicate parent = Predicate.create("https://oxfordsemantic.tech/RDFox/getting-started/Parent", 1);

        TGD fullTGDAboutParent = TGD.create(new Atom[] { Atom.create(hasParent, x, y) },
                new Atom[] { Atom.create(parent, y) });

        List<Rule> generatedRules = RDFoxFactory.createDatalogRule(fullTGDAboutParent);

        assertEquals(1, generatedRules.size());
        assertEquals(parsedRule, generatedRules.get(0));
        
    }

    @Test
    public void testRuleAboutName() throws ParsingException, IOException {
        DatalogParser parser = new DatalogParser(Prefixes.s_emptyPrefixes);
        parser.bind(getStreamFromResourceFile("rule-about-name.txt"));
        Rule parsedRule = parser.parseRule();

        Variable x = Variable.create("x");
        Variable y = Variable.create("y");
        Predicate forname = Predicate.create("https://oxfordsemantic.tech/RDFox/getting-started/forename", 2);
        Predicate name = Predicate.create("https://oxfordsemantic.tech/RDFox/getting-started/name", 2);

        TGD fullTGDAboutName = TGD.create(new Atom[] { Atom.create(forname, x, y) },
                new Atom[] { Atom.create(name, x, y) });

        List<Rule> generatedRules = RDFoxFactory.createDatalogRule(fullTGDAboutName);

        assertEquals(1, generatedRules.size());
        assertEquals(parsedRule, generatedRules.get(0));
        
    }
    
    public static FileInputStream getStreamFromResourceFile(String fileName) throws FileNotFoundException {
        return new FileInputStream(new File("src/test/resources/" + fileName).getAbsolutePath());
    }

}
