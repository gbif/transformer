package org.gbif.refine.datasets.nhmd;


import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.vocabulary.Rank;
import org.gbif.io.CSVReader;
import org.gbif.io.CSVReaderFactory;
import org.gbif.refine.utils.Constants;
import org.gbif.refine.utils.FileUtils;
import org.gbif.refine.utils.TermUtils;
import org.gbif.utils.file.ClosableReportingIterator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.validation.constraints.NotNull;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to clean, augment, and transform the original RooftopBugs dataset published by the Natural
 * History Museum of Denmark into a DwC sample-based, star format with event records with associated occurrences.
 */
public class RooftopBugs {

  private static final Logger LOG = LoggerFactory.getLogger(RooftopBugs.class);
  private static Map<String, NameUsage> names;

  public static void main(String[] args) throws IOException {
    // load list of all taxa
    names = loadTaxaList();
    LOG.info("Loaded " + names.size() + " unique canonical names.");

    // create directory where files should be written to
    File output = org.gbif.utils.file.FileUtils.createTempDir();

    // process all Lepidoptera records
    processLepidoptera(output);
    LOG.info("Processing Lepidoptera_1992-2009.csv complete! event.txt and occurrence.txt written to: " + output
      .getAbsolutePath());
  }

  /**
   * Loads a list of taxa from TaxaList.csv: each taxon gets converted into a NameUsage, and stored in a Map
   * organised by its canonical name (name without authorship).
   */
  public static Map<String, NameUsage> loadTaxaList() throws IOException {
    // load the original source file to process
    InputStream fis = RooftopBugs.class.getResourceAsStream("/datasets/nhmd/TaxaList-v2.csv");

    // create an iterator on the file
    CSVReader reader = CSVReaderFactory.build(fis, "UTF-8", ";", '"', 1);

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
        String part1 = Strings.nullToEmpty(record[2]).trim();
        if (!Strings.isNullOrEmpty(part1)) {
          nameUsage.setGenus(part1);
          canonicalName = canonicalName + part1;
        }

        // column 3: species (e.g. SpecificEpithet when rank=species)
        String part2 = Strings.nullToEmpty(record[3]).trim();
        if (!Strings.isNullOrEmpty(part2)) {
          if (nameUsage.getRank() != null) {
            nameUsage.setSpecies(part2);
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
    } catch (Exception e) {
      // some error validating this file, report
      LOG.error("Exception caught while iterating over file", e);
    } finally {
      if (iter != null) {
        iter.close();
      }
      reader.close();
    }

    return names;
  }

  /**
   * Iterates over original source file and does the following:
   * i) cleans it (e.g. maps column header names to DwC term names, converts dates to ISO format, etc)
   * ii) augments it (e.g. adds new columns for sample size, higher taxonomy, etc)
   * iii) transforms it into star format (core file events.txt is list of unique sampling events, and extension file
   * occurrence.txt is a list of all observations derived from all sampling events)
   *
   * @param output directory to write files to
   *
   * @throws IOException if method fails
   */
  public static void processLepidoptera(File output) throws IOException {
    // load the original source file to process
    InputStream fis = RooftopBugs.class.getResourceAsStream("/datasets/nhmd/Lepidoptera_1992-2009-v3.csv");

    // create an iterator on the file
    CSVReader reader = CSVReaderFactory.build(fis, "UTF-8", ";", '"', 1);

    // get header row for the new event and occurrence files that this method will output
    String[] header = getHeader();

    // sampling events file
    Writer writerEvents = FileUtils.startEventsFile(output, header);

    // observations file
    Writer writerOccs = FileUtils.startOccurrencesFile(output, header);

    // to capture all unique eventIDs
    Set<String> events = Sets.newHashSet();

    // to capture bad names
    Set<String> namesNotFound = Sets.newTreeSet();

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

        // create new augmented record
        String[] modifiedRecord = Arrays.copyOf(record, header.length);

        // add static values
        modifiedRecord[16] =
          "The material sample was collected, and either preserved or destructively processed."; // eventRemarks
        modifiedRecord[17] = "Denmark"; // country
        modifiedRecord[18] = "DK"; // countryCode
        modifiedRecord[19] = "Light trap on rooftop of Zoological Museum, Natural History Museum of Denmark (ZMUC)"; // locality
        modifiedRecord[20] = "55.702512"; // decimalLatitude
        modifiedRecord[21] = "12.558956"; // decimalLongitude
        modifiedRecord[22] = "WGS84"; // geodeticDatum
        modifiedRecord[23] = "modified Robinson light trap"; // samplingProtocol
        modifiedRecord[25] = "day"; // sampleSizeUnit
        modifiedRecord[27] = "http://creativecommons.org/licenses/by/4.0/legalcode"; // license
        modifiedRecord[28] = "Event"; // type
        modifiedRecord[29] = "Zoological Museum, Natural History Museum of Denmark (ZMUC)"; // rightsHolder
        modifiedRecord[30] = "ZMUC"; // institutionCode
        modifiedRecord[31] = "ZMUC"; // ownerInstitutionCode
        modifiedRecord[33] = "MaterialSample"; // basisOfRecord
        modifiedRecord[34] = "Ole Karsholt"; // recordedBy
        modifiedRecord[35] = "Ole Karsholt"; // identifiedBy
        modifiedRecord[37] = "individuals"; // organismQuantityType
        modifiedRecord[39] = "Animalia"; // kingdom
        modifiedRecord[40] = "Arthropoda"; // phylum
        modifiedRecord[41] = "Insecta"; // class

        // store organismQuantity even though it's the same as individualCount
        modifiedRecord[36] = modifiedRecord[8]; // value copied from individualCount

        // occurrenceStatus (present vs absent)
        modifiedRecord[38] = TermUtils.getOccurrenceStatus(Integer.valueOf(modifiedRecord[8])).toString().toLowerCase();

        // convert start date (e.g. 21/08/94) into ISO format
        String start = modifiedRecord[2];
        DateFormat df = new SimpleDateFormat("dd/MM/yy", new Locale("dk", "DK"));
        Date startDate = df.parse(start);
        modifiedRecord[2] = Constants.ISO_DF.format(startDate);

        // convert end date (e.g. 21/08/94) into ISO format
        String end = modifiedRecord[3];
        Date endDate = df.parse(end);
        modifiedRecord[3] = Constants.ISO_DF.format(endDate);

        // combine start and end date into date range for eventDate
        modifiedRecord[15] = modifiedRecord[2] + "/" + modifiedRecord[3];

        // calculate samplingEffort in number of trap days
        long diff = endDate.getTime() - startDate.getTime();
        float days = diff / (24 * 60 * 60 * 1000);
        modifiedRecord[26] = String.valueOf(Math.round(days)) + " trap day(s)";

        // store sampleSize even though it's the same as samplingEffort
        modifiedRecord[24] = String.valueOf(Math.round(days));

        // construct unique eventID for this sampling period
        // Format: "urn:[institutionID]:[startDate/endDate]"
        // Example: "urn:zmuc:1994-08-12/1994-08-21"
        modifiedRecord[49] = "urn:zmuc:" + modifiedRecord[15];

        // find name in taxa list
        String name = modifiedRecord[4].trim();
        // only use canonical name in lookup
        String[] parts = name.split(" ");
        if (parts.length >= 2) {
          String canonical = parts[0].trim();

          // exclude "sp." from canonical name
          String specificEpithet = parts[1].trim();
          if (!specificEpithet.equals("sp.")) {
            canonical+= " " + specificEpithet;
          }

          NameUsage found = names.get(canonical);
          if (found != null) {
            modifiedRecord[43] = found.getGenus();
            modifiedRecord[44] = found.getCanonicalName() + " " + found.getAuthorship();
            modifiedRecord[45] = found.getAuthorship();
            modifiedRecord[46] = (found.getRank() == null) ? null : found.getRank().toString().toLowerCase();
            modifiedRecord[47] = found.getTaxonID();

            // names that changed store previous identification in "previousIdentifications"
            if (canonical.equals("Bena bicolorana")) {
              modifiedRecord[50] = "Bena prasinana L.";
            } else if (canonical.equals("Pseudoips prasinana")) {
              modifiedRecord[50] = "Pseudoips fagana F.";
            }
          } else {
            if (!namesNotFound.contains(name)) {
              namesNotFound.add(name);
            }
          }
        } else {
          LOG.error("*****Bad species name encountered: " + name);
        }

        // construct unique occurrenceID for this abundance record:
        // Format: "urn:[institutionCode]:[startDate/endDate]:[taxonID]"
        // Example: "urn:zmuc:1994-08-12/1994-08-21:1301"
        modifiedRecord[32] = modifiedRecord[49] + ":" + modifiedRecord[47];

        // always output line to new occurrences file
        String row = FileUtils.tabRow(modifiedRecord);
        writerOccs.write(row);

        // only output line to events file if event hasn't been included yet
        String eventID = modifiedRecord[2];
        if (!events.contains(eventID)) {
          writerEvents.write(row);
          events.add(eventID);
        }
      }
      LOG.info("Iterated over " + line + " rows.");
      LOG.info("Found " + events.size() + " unique events.");

      LOG.warn("***** " + namesNotFound.size() + " names not found in taxa list: ");
      for (String notFound : namesNotFound) {
        LOG.warn(notFound);
      }

    } catch (Exception e) {
      // some error validating this file, report
      LOG.error("Exception caught while iterating over file", e);
    } finally {
      if (iter != null) {
        iter.close();
      }
      reader.close();
      writerEvents.close();
      writerOccs.close();
    }
  }

  /**
   * @return array of column names in output files (event.txt, occurrence.txt)
   */
  @NotNull
  private static String[] getHeader() {
    String[] header = new String[51];

    // ***original columns

    // header 0: order, e.g. LEPIDOPTERA
    // maps to dwc:order
    header[0] = "order";
    // header 1: group, e.g. ACROLEPIIDAE
    header[1] = "group";
    // header 2: date1, e.g. 12/08/94
    // converted to ISO format 1994-08-12
    header[2] = "date1";
    // header 3: date2, e.g. 21/08/94
    // converted to ISO format 1994-08-21
    header[3] = "date2";
    // header 4: name, e.g. Acrolepiopsis assectella Zell.:
    header[4] = "name";
    // header 5: year, e.g. 1994
    // maps to dwc:year (must pair with dwc:organismQuantityType)
    header[5] = "year";
    // header 6: w, e.g. 1 ex 12.-21.viii.
    header[6] = "w";
    // header 7: i, e.g. 1
    header[7] = "i";
    // header 8: Antal, e.g. 1
    // Total abundance for species recorded during that trap event
    // maps to dwc:individualCount (must pair with dwc:organismQuantityType)
    header[8] = "individualCount";
    // header 9: month1, e.g. 8
    header[9] = "month1";
    // header 10: day1, e.g. 12
    header[10] = "day1";
    // header 11: month2, e.g. 8
    header[11] = "month2";
    // header 12: day2, e.g. 21
    header[12] = "day2";
    // header 13: no_one, e.g. 1
    header[13] = "no_one";
    // header 14: identificationRemarks, e.g. "Either species a or b"
    header[14] = "identificationRemarks";

    // ***new augmented columns of information

    // eventDate range, e.g. 1994-08-12/1994-08-21
    header[15] = "eventDate";
    // The material sample was collected, and either preserved or destructively processed.
    header[16] = "eventRemarks";
    // Denmark
    header[17] = "country";
    // DK
    header[18] = "countryCode";
    // Rooftop of Natural History Museum of Denmark
    header[19] = "locality";
    // 55·702512
    header[20] = "decimalLatitude";
    // 12·558956
    header[21] = "decimalLongitude";
    // WGS84
    header[22] = "geodeticDatum";
    // modified Robinson light trap
    header[23] = "samplingProtocol";
    // time duration in number of trap days
    header[24] = "sampleSizeValue";
    // day
    header[25] = "sampleSizeUnit";
    // number of trap days
    header[26] = "samplingEffort";
    // http://creativecommons.org/licenses/by/4.0/legalcode
    header[27] = "license";
    // Event
    header[28] = "type";
    // Natural History Museum of Denmark
    header[29] = "rightsHolder";
    // ZMUC
    header[30] = "institutionCode";
    // ZMUC
    header[31] = "ownerInstitutionCode";
    // unique occurrenceID
    header[32] = "occurrenceID";
    // MaterialSample
    header[33] = "basisOfRecord";
    // Ole Karsholt
    header[34] = "recordedBy";
    // Ole Karsholt
    header[35] = "identifiedBy";
    // copied from individualCount
    header[36] = "organismQuantity";
    // individuals
    header[37] = "organismQuantityType";
    // present or absent - depending on individualCount
    header[38] = "occurrenceStatus";
    // taxonomy
    header[39] = "kingdom";
    header[40] = "phylum";
    header[41] = "class";
    header[42] = "family";
    header[43] = "genus";
    header[44] = "scientificName";
    header[45] = "scientificNameAuthorship";
    header[46] = "taxonRank";
    header[47] = "taxonID";
    header[48] = "taxonomicStatus";

    // unique eventID
    header[49] = "eventID";
    // to capture name change
    header[50] = "previousIdentifications";

    // TODO: minimum/maximumElevationInMeters

    return header;
  }
}
