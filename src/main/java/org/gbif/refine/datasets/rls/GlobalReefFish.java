package org.gbif.refine.datasets.rls;


import org.gbif.api.model.checklistbank.NameUsage;
import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.model.common.LinneanClassification;
import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.vocabulary.Country;
import org.gbif.api.vocabulary.Rank;
import org.gbif.common.parsers.CountryParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.io.CSVReader;
import org.gbif.io.CSVReaderFactory;
import org.gbif.refine.client.WebserviceClientModule;
import org.gbif.refine.utils.FileUtils;
import org.gbif.refine.utils.TermUtils;
import org.gbif.utils.file.ClosableReportingIterator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.Arrays;
import java.util.Set;
import javax.validation.constraints.NotNull;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to clean, augment, and transform the original Global Reef Fish dataset published by Reef Life
 * Survey into a DwC sample event, star-formatted dataset consisting of event records (core records) and their associated
 * occurrences (extension records).
 */
public class GlobalReefFish {

  private static final Logger LOG = LoggerFactory.getLogger(GlobalReefFish.class);
  private static final CountryParser COUNTRY_PARSER = CountryParser.getInstance();
  private static final NameUsageMatchingService MATCHING_SERVICE =
    WebserviceClientModule.webserviceClientReadOnly().getInstance(NameUsageMatchingService.class);

  public static void main(String[] args) throws IOException {
    // directory where files should be written to
    File output = org.gbif.utils.file.FileUtils.createTempDir();
    processGlobalReefFish(output);
    LOG.info(
      "Processing M1_DATA-100.csv complete! event.txt and occurrence.txt written to: " + output.getAbsolutePath());
  }

  /**
   * Iterates over original source file and does the following:
   * i) cleans it (e.g. maps column header names to DwC term names, matching scientific names to GBIF Backbone
   * Taxonomy)
   * ii) augments it (e.g. adds new columns for sample size, higher taxonomy, etc)
   * iii) transforms it into star format (core file events.txt is list of unique sampling events, and extension file
   * occurrence.txt is a list of all observations derived from all sampling events)
   *
   * @param output directory to write files to
   *
   * @throws IOException if method fails
   */
  public static void processGlobalReefFish(File output) throws IOException {
    // load the original source file to process
    InputStream fis = GlobalReefFish.class.getResourceAsStream("/datasets/rls/M1_DATA-1.csv");

    // create an iterator on the file
    CSVReader reader = CSVReaderFactory.build(fis, "UTF-8", ",", '"', 1);

    // get header row for the new event and occurrence files that this method will output
    String[] header = getHeader();

    // sampling events file
    Writer writerEvents = FileUtils.startEventsFile(output, header);

    // observations file
    Writer writerOccs = FileUtils.startOccurrencesFile(output, header);

    // to capture all unique eventIDs
    Set<String> events = Sets.newHashSet();

    // to capture all names that don't match GBIF Backbone Taxonomy
    Set<String> nonMatchingNames = Sets.newHashSet();

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
        modifiedRecord[20] = "500"; // sampleSizeValue
        modifiedRecord[21] = "square_metre"; // sampleSizeUnit
        modifiedRecord[22] = "Reef Life Survey methods"; // samplingProtocol
        modifiedRecord[23] = "http://creativecommons.org/licenses/by/4.0/legalcode"; // license
        modifiedRecord[24] = "Event"; // type
        modifiedRecord[25] = "WGS84"; // geodeticDatum
        modifiedRecord[26] = "individuals"; // organismQuantityType
        modifiedRecord[33] = "RLS"; // rightsHolder
        modifiedRecord[34] = "RLS"; // institutionCode
        modifiedRecord[35] = "RLS"; // ownerInstitutionCode
        modifiedRecord[36] = "HumanObservation"; // basisOfRecord
        modifiedRecord[38] = "Two blocks form a complete transect"; // eventRemarks

        // indicate mean depth is in meters
        String meanDepth = modifiedRecord[11];
        modifiedRecord[11] = meanDepth + " m";

        // indicate which Block (of two) observation was made in
        String blockNumber = modifiedRecord[16];
        modifiedRecord[16] = "Observed in block #" + blockNumber;

        // occurrenceStatus (present vs absent)
        modifiedRecord[27] =
          TermUtils.getOccurrenceStatus(Integer.valueOf(modifiedRecord[17])).toString().toLowerCase();

        // construct higherGeography using formula: Country | Realm | Ecoregion
        String country = modifiedRecord[3];
        String realm = modifiedRecord[5];
        String ecoregion = modifiedRecord[4];
        modifiedRecord[28] = country + " | " + realm + " | " + ecoregion;

        // store individualCount even though it's the same as organismQuantity
        modifiedRecord[37] = modifiedRecord[17]; // value copied from organismQuantity

        // add 2 letter ISO country code
        ParseResult<Country> result = COUNTRY_PARSER.parse(modifiedRecord[3]);
        if (result.isSuccessful()) {
          modifiedRecord[39] = result.getPayload().getIso2LetterCode();
        }

        // verify taxonomy
        String name = modifiedRecord[15];

        // for more accurate match, we take higher taxonomy into consideration
        LinneanClassification cl = new NameUsage();
        cl.setPhylum(modifiedRecord[12]);
        cl.setClazz(modifiedRecord[13]);
        cl.setFamily(modifiedRecord[14]);
        // only if binomial, set species
        if (name.split(" ").length == 2 && !name.endsWith("spp.")) {
          cl.setSpecies(name);
        }

        // lowest rank specified
        Rank rank = TermUtils.lowestRank(cl);
        if (rank != null) {
          modifiedRecord[29] = rank.toString();
        }

        // verify name, and add higher taxonomy
        NameUsageMatch match = MATCHING_SERVICE.match(name, rank, cl, false, false);
        if (match.getMatchType().equals(NameUsageMatch.MatchType.EXACT)) {
          modifiedRecord[30] = match.getStatus().toString();
          modifiedRecord[31] = match.getKingdom();
          modifiedRecord[32] = match.getOrder();
        } else {
          if (!nonMatchingNames.contains(name)) {
            LOG.error(
              match.getMatchType().toString() + " match for: " + name + " (with rank " + rank + ") to: " + match
                .getScientificName() + " (with rank " + match.getRank() + ")" + ". See example record with FID: "
              + modifiedRecord[0]);
            nonMatchingNames.add(name);
          }
        }

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
      LOG.error("Found " + nonMatchingNames.size() + " non-matching names.");
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
    String[] header = new String[40];

    // header 0: FID, e.g. M1_DATA.1
    // RLS definition: Non-stable record-level identifier
    // maps to dwc:occurrenceID
    header[0] = "occurrenceID";

    // header 1: Key, e.g. 1
    // no mapping to DwC
    header[1] = "Key";

    // header 2: SurveyID, e.g. 912344644
    // RLS definition: Identifier of individual 50 m transects
    // maps to dwc:eventID
    header[2] = "eventID";

    // header 3: Country, e.g. Indonesia
    // RLS definition: Country (or largely-autonomous state)
    // maps to dwc:country
    header[3] = "country";

    // header 4: Ecoregion, e.g. Lesser Sunda
    // RLS definition: Location within the Marine Ecoregions of the World provided in Spalding et al.13.
    // no direct mapping to DwC, but used to construct dwc:higherGeography
    header[4] = "Ecoregion";

    // header 5: Realm, e.g. Cenral Indo-Pacific
    // RLS definition: Biogeographic realm as classified in the Marine Ecoregions of the World13
    // no direct mapping to DwC, used to construct dwc:higherGeography
    header[5] = "Realm";

    // header 6: SiteCode, e.g. BALI2
    // RLS definition: Identifier of unique geographical coordinates
    // maps to dwc:locationID
    header[6] = "locationID";

    // header 7: Site, e.g. Paradise House reef
    // RLS definition: Descriptive name of the site
    // maps to dwc:locality
    header[7] = "locality";

    // header 8: SiteLat, e.g. -8.2773
    // RLS definition: Latitude of site (WGS84)
    // maps to dwc:decimalLatitude
    header[8] = "decimalLatitude";

    // header 9: SiteLong, e.g. 115.5945
    // RLS definition: Longitude of site (WGS84)
    // maps to dwc:decimalLongitude
    header[9] = "decimalLongitude";

    // header 10: SurveyDate, e.g. 2014-10-26T00:00:00
    // RLS definition: Date of survey
    // maps to dwc:eventDate
    header[10] = "eventDate";

    // header 11: Depth, e.g. 4 metres
    // RLS definition: Mean depth of transect line as recorded on dive computer (note: this does not account for tide or deviations from the mean value as a consequence of imperfect tracking of the depth contour along the bottom))
    // maps to dwc:verbatimDepth
    header[11] = "verbatimDepth"; // TODO: should append m for metres

    // header 12: Phylum
    // RLS definition: Taxonomic Phylum
    // maps to dwc:phylum
    header[12] = "phylum";

    // header 13: Class
    // RLS definition: Taxonomic Class
    // maps to dwc:class
    header[13] = "class";

    // header 14: Family
    // RLS definition: Taxonomic Family
    // maps to dwc:family
    header[14] = "family";

    // header 15: Taxon
    // RLS definition: Species name, corrected for recent taxonomic changes and grouping of records not at species level)
    // maps to dwc:scientificName
    header[15] = "scientificName";

    // header 16: Block
    // RLS definition: Identifies which 5 m wide block (of two) within each complete transect (surveyID) - Values = 1 (block on deeper/offshore side of transect line) , 2 (block on shallower/inshore side))
    // maps to dwc:occurrenceRemarks (preceded with Block + n)
    header[16] = "occurrenceRemarks";

    // header 17: Total
    // RLS definition: Total abundance for record on that block, transect, site, date combination
    // maps to dwc:organismQuantity (must pair with dwc:organismQuantityType)
    header[17] = "organismQuantity";

    // header 18: Diver
    // RLS definition: Initials of the diver who collected the datum
    // maps to dwc:recordedBy
    header[18] = "recordedBy";

    // header 19: geom
    // RLS definition: WKT POINT
    // maps to dwc:footprintWKT
    header[19] = "footprintWKT";

    // additional DwC columns
    header[20] = "sampleSizeValue";
    header[21] = "sampleSizeUnit";
    header[22] = "samplingProtocol";
    header[23] = "license";
    header[24] = "type";
    header[25] = "geodeticDatum";
    header[26] = "organismQuantityType";
    header[27] = "occurrenceStatus";
    header[28] = "higherGeography";
    header[29] = "taxonRank";
    header[30] = "taxonomicStatus";
    header[31] = "kingdom";
    header[32] = "order";
    header[33] = "rightsHolder";
    header[34] = "institutionCode";
    header[35] = "ownerInstitutionCode";
    header[36] = "basisOfRecord";
    header[37] = "individualCount";
    header[38] = "eventRemarks";
    header[39] = "countryCode";
    // TODO add info about MEOW Marine Ecosystems of the World classification

    return header;
  }
}
