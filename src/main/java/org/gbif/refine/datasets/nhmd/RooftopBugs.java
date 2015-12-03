package org.gbif.refine.datasets.nhmd;


import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.vocabulary.Rank;
import org.gbif.io.CSVReader;
import org.gbif.io.CSVReaderFactory;
import org.gbif.utils.file.ClosableReportingIterator;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to clean, augment, and transform the original RooftopBugs dataset published by the Natural
 * History Museum of Denmark into a DwC sample-based, star format with event records with associated occurrences.
 */
public class RooftopBugs {

  private static final Logger LOG = LoggerFactory.getLogger(RooftopBugs.class);

  public static void main(String[] args) throws IOException {
    loadTaxaList();
  }

  /**
   * Loads a list of taxa from TaxaList.csv: each taxon gets converted into a NameUsage, and stored in a Map
   * organised by its canonical name (name without authorship).
   */
  public static void loadTaxaList() throws IOException {
    // load the original source file to process
    InputStream fis = RooftopBugs.class.getResourceAsStream("/datasets/nhmd/TaxaList.csv");

    // create an iterator on the file
    CSVReader reader = CSVReaderFactory.build(fis, "Latin1", ";", '"', 1);

    // to capture all NameUsages into a map with their canonical name as key
    Map<String, NameUsage> names = Maps.newHashMap();

    ClosableReportingIterator<String[]> iter = null;
    int line = 0;
    try {
      iter = reader.iterator();
      while (iter.hasNext()) {
        line++;
        String[] record = iter.next();
        if (record == null || record.length == 0) {
          continue;
        }

        NameUsage nameUsage = new NameUsage();
        String canonicalName = "";

        // column 0: taxonID
        String taxonID = Strings.nullToEmpty(record[0]);
        if (!Strings.isNullOrEmpty(taxonID)) {
          nameUsage.setTaxonID(taxonID);
        }

        // column 1: shorthand rank
        String taxonRank = Strings.nullToEmpty(record[1]);
        if (!Strings.isNullOrEmpty(taxonRank)) {
          // interpret Rank
          if (taxonRank.equalsIgnoreCase("FAMIL")) {
            nameUsage.setRank(Rank.FAMILY);
          } else if (taxonRank.equalsIgnoreCase("SUBFA")) {
            nameUsage.setRank(Rank.SUBFAMILY);
          } else if (taxonRank.equalsIgnoreCase("TRIBU")) {
            nameUsage.setRank(Rank.TRIBE);
          } else if (taxonRank.equalsIgnoreCase("SUPER")) {
            nameUsage.setRank(Rank.SUPERFAMILY);
          } else if (taxonRank.equalsIgnoreCase("GENUS")) {
            nameUsage.setRank(Rank.GENUS);
          } else if (taxonRank.equalsIgnoreCase("SPECI")) {
            nameUsage.setRank(Rank.SPECIES);
          } else {
            LOG.error("Failed to match shorthand rank: " + taxonRank);
          }
        }

        // column 2: Genus when rank=species
        String part1 = Strings.nullToEmpty(record[2]);
        if (!Strings.isNullOrEmpty(part1)) {
          nameUsage.setGenus(part1);
          canonicalName = canonicalName + part1;
        }

        // column 3: scientificName (e.g. SpecificEpithet when rank=species)
        String part2 = Strings.nullToEmpty(record[3]);
        if (!Strings.isNullOrEmpty(part2)) {
          if (nameUsage.getRank() != null) {
            nameUsage.setScientificName(part2);
            canonicalName = canonicalName + " " + part2;
          }
        }

        // column 4: scientificNameAuthorship
        String scientificNameAuthorship = Strings.nullToEmpty(record[4]);
        if (!Strings.isNullOrEmpty(scientificNameAuthorship)) {
          nameUsage.setAuthorship(scientificNameAuthorship);
        }

        // name without authorship
        if (!Strings.isNullOrEmpty(canonicalName)) {
          canonicalName = canonicalName.trim();
          nameUsage.setCanonicalName(canonicalName);
          if (!names.containsKey(canonicalName)) {
            names.put(canonicalName, nameUsage);
          } else {
            LOG.warn("Map already contains NameUsage with canonical name: " + canonicalName);
          }
        } else {
          LOG.warn("Taxon has no canonical name - check line: " + String.valueOf(line));
        }
      }
      LOG.info("Iterated over " + line + " lines in TaxaList.csv.");
      LOG.info("Found " + names.size() + " unique canonical names.");
    } catch (Exception e) {
      // some error validating this file, report
      LOG.error("Exception caught while iterating over file", e);
    } finally {
      if (iter != null) {
        iter.close();
      }
      reader.close();
    }
  }
}
