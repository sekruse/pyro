package de.hpi.isg.pyro.significance.histograms;

import java.io.PrintStream;

/**
 * Specifies some function that can be plotted against a range.
 */
public interface CsvPrintableFunction {

    public void plotAgainstAsCsv(HistogramDomain domain, PrintStream out);

}
