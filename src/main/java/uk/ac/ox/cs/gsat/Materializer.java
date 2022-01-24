package uk.ac.ox.cs.gsat;

import java.io.OutputStream;
import java.util.Collection;

import uk.ac.ox.cs.pdq.fol.TGD;


/**
 * Materialize the facts induced by a set of full TGDs and output them 
 */
public interface Materializer {

    /**
     * Returns the number of materialized facts
     */
    public long materialize(String inputDataFile, Collection<TGD> fullTGDs, OutputStream outputStream) throws Exception;

    /**
     * Returns the number of materialized facts
     */
    public long materialize(String inputDataFile, Collection<TGD> fullTGDs, String outputFile) throws Exception;
}
