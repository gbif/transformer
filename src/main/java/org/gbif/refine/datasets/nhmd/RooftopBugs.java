package org.gbif.refine.datasets.nhmd;


import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.model.common.LinneanClassification;
import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.vocabulary.Rank;
import org.gbif.io.CSVReader;
import org.gbif.io.CSVReaderFactory;
import org.gbif.refine.client.WebserviceClientModule;
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
import java.util.Calendar;
import java.util.Collections;
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
  private static Set<String> events;
  private static Set<String> validColeopteraNamesNotInNub;

  private static final NameUsageMatchingService MATCHING_SERVICE =
    WebserviceClientModule.webserviceClientReadOnly().getInstance(NameUsageMatchingService.class);

  private static String lepidopteraEventsFileName = "events-lepidoptera.tab";
  private static String lepidopteraOccurrencesFileName = "occurrences-lepidoptera.tab";
  private static String coleopteraOccurrencesFileName = "occurrences-coleoptera.tab";

  public static void main(String[] args) throws IOException {
    // load list of all taxa
    names = loadTaxaList();
    LOG.info("Loaded " + names.size() + " unique canonical names.");

    // set of eventIDs
    events = Sets.newHashSet();

    // valid verified names not existing in GBIF Backbone Taxonomy (Nub)
    validColeopteraNamesNotInNub = Collections.unmodifiableSet(Sets
      .newHashSet("Acanthocinus griseus (Fabricius, 1792)", "Aphodius rufipes (Linnaeus, 1758)",
        "Aphodius rufus (Moll, 1782)", "Aphodius sordidus (Fabricius, 1775)", "Curculio glandium Marsham, 1802",
        "Curculio nucum Linnaeus, 1758", "Dorytomus rufatus (Bedel, 1886)", "Dorytomus taeniatus (Fabricius, 1781)",
        "Hylobius abietis (Linnaeus, 1758)", "Magdalis barbicornis (Latreille, 1804)",
        "Magdalis ruficornis (Linnaeus, 1758)", "Phytobius leucogaster (Marsham, 1802)"));

    // create directory where files should be written to
    File output = org.gbif.utils.file.FileUtils.createTempDir();

    // first, process all Lepidoptera records (order is important)
    processLepidoptera(output);
    LOG.info("Processing Lepidoptera_1992-2009.csv complete! " + lepidopteraEventsFileName + " and "
             + lepidopteraOccurrencesFileName + " written to: " + output.getAbsolutePath());

    // second, process all Coleoptera record
    processColeoptera(output);
    LOG.info("Processing Coleoptera_1992-2009.csv complete! " + coleopteraOccurrencesFileName + " written to: "
             + output.getAbsolutePath());
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
    String[] header = getLepidopteraHeader();

    // sampling events file
    Writer writerEvents = FileUtils.startEventsFile(output, header, lepidopteraEventsFileName);

    // observations file
    Writer writerOccs = FileUtils.startOccurrencesFile(output, header, lepidopteraOccurrencesFileName);

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

        // eventID for this sampling period
        modifiedRecord[49] = constructEventID(modifiedRecord[15]);

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
        if (!events.contains(modifiedRecord[49])) {
          writerEvents.write(row);
          events.add(modifiedRecord[49]);
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
  public static void processColeoptera(File output) throws IOException {
    // load the original source file to process
    InputStream fis = RooftopBugs.class.getResourceAsStream("/datasets/nhmd/Coleoptera_1992-2009-v3.csv");

    // create an iterator on the file
    CSVReader reader = CSVReaderFactory.build(fis, "UTF-8", ";", '"', 1);

    // get header row for the new event and occurrence files that this method will output
    String[] header = getColeopteraHeader();

    // observations file
    Writer writerOccs = FileUtils.startOccurrencesFile(output, header, coleopteraOccurrencesFileName);

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
        modifiedRecord[37] = "individuals"; // organismQuantityType
        modifiedRecord[39] = "Animalia"; // kingdom
        modifiedRecord[40] = "Arthropoda"; // phylum
        modifiedRecord[41] = "Insecta"; // class

        // store organismQuantity even though it's the same as individualCount
        modifiedRecord[36] = modifiedRecord[6]; // value copied from individualCount

        // occurrenceStatus (present vs absent)
        modifiedRecord[38] = TermUtils.getOccurrenceStatus(Integer.valueOf(modifiedRecord[6])).toString().toLowerCase();

        // convert start date (e.g. 5/17/93) into ISO format
        String start = modifiedRecord[4];
        DateFormat df = new SimpleDateFormat("MM/dd/yy", new Locale("dk", "DK"));
        Date startDate = df.parse(start);
        modifiedRecord[4] = Constants.ISO_DF.format(startDate);

        // convert end date (e.g. 5/23/93) into ISO format
        String end = modifiedRecord[5];
        Date endDate = df.parse(end);
        modifiedRecord[5] = Constants.ISO_DF.format(endDate);

        // combine start and end date into date range for eventDate
        modifiedRecord[15] = modifiedRecord[4] + "/" + modifiedRecord[5];

        // calculate samplingEffort in number of trap days
        long diff = endDate.getTime() - startDate.getTime();
        float days = diff / (24 * 60 * 60 * 1000);
        modifiedRecord[26] = String.valueOf(Math.round(days)) + " trap day(s)";

        // store sampleSize even though it's the same as samplingEffort
        modifiedRecord[24] = String.valueOf(Math.round(days));

        // all Coleoptera recorded between 1992 and 1999 were identified by Michael Hansen, then Jan Pedersen took over
        Calendar c = Calendar.getInstance();
        c.set(Calendar.YEAR, 2000);
        c.set(Calendar.DAY_OF_YEAR, 1);
        Date mm = c.getTime();
        modifiedRecord[35] = (startDate.before(mm)) ? "Michael Hansen" : "Jan Pedersen"; // identifiedBy

        // eventID for this sampling period
        modifiedRecord[49] = constructEventID(modifiedRecord[15]);

        // verify taxonomy
        String name = modifiedRecord[2].trim();

        // for more accurate match, we take higher taxonomy into consideration
        LinneanClassification cl = new NameUsage();
        cl.setKingdom(modifiedRecord[39]); // static
        cl.setPhylum(modifiedRecord[40]); // static
        cl.setClazz(modifiedRecord[41]); // static
        cl.setOrder(modifiedRecord[0]);
        cl.setSpecies(name);

        // lowest rank specified
        Rank rank = TermUtils.lowestRank(cl);
        if (rank != null) {
          modifiedRecord[46] = rank.toString();
        }

        // verify name, and add higher taxonomy
        NameUsageMatch match = MATCHING_SERVICE.match(name, rank, cl, false, false);
        if (validColeopteraNamesNotInNub.contains(name)) {
          // skip
        } else if (match.getMatchType().equals(NameUsageMatch.MatchType.EXACT)) {
          modifiedRecord[48] = match.getStatus().toString();
          modifiedRecord[42] = match.getFamily();
          modifiedRecord[43] = match.getGenus();
          modifiedRecord[44] = match.getScientificName();
          modifiedRecord[47] = match.getUsageKey().toString();
        } else {
          if (!namesNotFound.contains(name)) {
            LOG.error(
              match.getMatchType().toString() + " match for: " + name + " (with rank " + rank + ") to: " + match
                .getScientificName() + " (with rank " + match.getRank() + ")");
            namesNotFound.add(name);
          }
        }

        // construct unique occurrenceID for this abundance record:
        // Format: "urn:[institutionCode]:[startDate/endDate]:[taxonID]"
        // Example: "urn:zmuc:1994-08-12/1994-08-21:1301"
        modifiedRecord[32] = modifiedRecord[49] + ":" + modifiedRecord[47];

        // always output line to new occurrences file
        String row = FileUtils.tabRow(modifiedRecord);
        writerOccs.write(row);

        // all Coleoptera sampling events are a subset of all Lepidoptera sampling events
        if (!events.contains(modifiedRecord[49])) {
          LOG.error("Sampling event not found: " + modifiedRecord[49]);
        }
      }
      LOG.info("Iterated over " + line + " rows.");

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
      writerOccs.close();
    }
  }

  /**
   * @return array of column names in output files for Lepidoptera data (event.txt, occurrence.txt)
   */
  @NotNull
  private static String[] getLepidopteraHeader() {
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
    // maps to dwc:year
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

    // ***new augmented columns of information

    // header 14: identificationRemarks, e.g. "Either species a or b"
    header[14] = "identificationRemarks";
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
    header[42] = "gbif_family";
    header[43] = "genus";
    header[44] = "scientificName";
    header[45] = "scientificNameAuthorship";
    header[46] = "taxonRank";
    header[47] = "taxonID";
    header[48] = "gbif_taxonomicStatus";

    // unique eventID
    header[49] = "eventID";
    // to capture name change
    header[50] = "previousIdentifications";

    // TODO: minimum/maximumElevationInMeters

    return header;
  }

  /**
   * @return array of column names in output files for Coleoptera data (event.txt, occurrence.txt)
   */
  @NotNull
  private static String[] getColeopteraHeader() {
    String[] header = new String[51];

    // ***original columns

    // header 0: order, e.g. COLEOPTERA
    // maps to dwc:order
    header[0] = "order";
    // header 1: group, e.g. ADERIDAE
    header[1] = "group";
    // header 2: name, e.g. Aderus populneus (Creutzer)
    header[2] = "name";
    // header 3: year, e.g. 1993
    // maps to dwc:year
    header[3] = "year";
    // header 4: date1, e.g. 5/17/93
    // converted to ISO format 1993-05-17
    header[4] = "date1";
    // header 5: date2, e.g. 5/23/93
    // converted to ISO format 1994-05-23
    header[5] = "date2";
    // header 6: individuals, e.g. 1
    // Total abundance for species recorded during that trap event
    // maps to dwc:individualCount (must pair with dwc:organismQuantityType)
    header[6] = "individualCount";
    // header 7: month1, e.g. 5
    header[7] = "month1";
    // header 8: day1, e.g. 17
    header[8] = "day1";
    // header 9: month2, e.g. 5
    header[9] = "month2";
    // header 10: day2, e.g. 23
    header[10] = "day2";
    // header 11: startday, e.g. 137
    header[11] = "startday";
    // header 12: endday, e.g. 143
    header[12] = "endday";
    // header 13: diff, e.g. 7
    header[13] = "diff";
    // header 14: newname, e.g. 44
    header[14] = "newname";

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
    header[42] = "gbif_family";
    header[43] = "gbif_genus";
    header[44] = "gbif_scientificName";
    header[45] = "gbif_scientificNameAuthorship";
    header[46] = "taxonRank";
    header[47] = "gbif_taxonID";
    header[48] = "gbif_taxonomicStatus";

    // unique eventID
    header[49] = "eventID";
    // to capture name change
    header[50] = "previousIdentifications";

    // TODO: minimum/maximumElevationInMeters

    return header;
  }

  /**
   * Construct unique eventID for this sampling period using format: "urn:[institutionID]:[startDate/endDate]". E.g.
   * "urn:zmuc:1994-08-12/1994-08-21"
   *
   * @param eventDate event date
   *
   * @return eventID
   */
  private static String constructEventID(@NotNull String eventDate) {
    return "urn:zmuc:" + eventDate;
  }
}
