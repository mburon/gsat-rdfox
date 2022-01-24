package uk.ac.ox.cs.gsat.rdfox;

import uk.ac.ox.cs.gsat.rdfox.statistics.StatisticsColumn;

public enum MaterializationStatColumns implements StatisticsColumn {

    // number of full tgds used for the materialization  
    MAT_FTGD_NB,
    // size of the generated input
    MAT_GEN_SIZE,
    // size of the materialization
    MAT_SIZE,
    // time required to generated the input
    MAT_GEN_TIME,
    // time of the materialization
    MAT_TIME

}
