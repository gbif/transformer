package org.gbif.refine.datasets.taibif;

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
import java.util.Date;
import java.util.Locale;
import java.util.Set;
import javax.validation.constraints.NotNull;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to clean, augment, and transform the original Fish Assemblages dataset published in Scientific
 * Data into a DwC sample event, star-formatted dataset consisting of event records (core records) and their associated
 * occurrences (extension records).
 */
public class FishAssemblages {

  private static final Logger LOG = LoggerFactory.getLogger(FishAssemblages.class);
  private static final NameUsageMatchingService MATCHING_SERVICE =
    WebserviceClientModule.webserviceClientReadOnly().getInstance(NameUsageMatchingService.class);

  public static void main(String[] args) throws IOException {
    // directory where files should be written to
    File output = org.gbif.utils.file.FileUtils.createTempDir();
    processFish(output);
    LOG.info(
      "Processing 1987-1990_UTF8.txt complete! event.txt and occurrence.txt written to: " + output.getAbsolutePath());
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
  public static void processFish(File output) throws IOException {
    // load the original source file to process
    InputStream fis = FishAssemblages.class.getResourceAsStream("/datasets/taibif/1987-1990_UTF8.txt");

    // create an iterator on the file
    CSVReader reader = CSVReaderFactory.build(fis, "UTF-8", "\t", null, 1);

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

        // convert year and month into ISO format
        String year = modifiedRecord[1];
        String month = modifiedRecord[2];

        if (year.length() == 4 && month.length() == 3) {
          String concatenatedDate = year + "-" + month;
          DateFormat concatenatedDf = new SimpleDateFormat("yy-MMM", Locale.ENGLISH);
          Date concatenatedEventDate = concatenatedDf.parse(concatenatedDate);
          String concatenatedIsoDate = Constants.ISO_DF_SHORT.format(concatenatedEventDate);

          // quality control: ensure year and month are same as eventDate (if eventDate provided)
          String verbatimEventDate = modifiedRecord[3];
          if (!verbatimEventDate.isEmpty()) {

            // convert event date (e.g. 1987/03/) into ISO format (e.g. 1987-03)
            DateFormat df = new SimpleDateFormat("yy/MM/", Locale.ENGLISH);
            Date eventDate = df.parse(verbatimEventDate);
            String isoDate = Constants.ISO_DF_SHORT.format(eventDate);

            if (!isoDate.equals(concatenatedIsoDate)) {
              LOG.error("Skipping record: year " + year + " & month " + month + " don't match eventDate " + isoDate);
              continue;
            }
          }
          modifiedRecord[3] = concatenatedIsoDate;
        } else {
          LOG.error("Skipping record: invalid year (" + year + ") and month (" + month + ")");
          continue;
        }

        modifiedRecord[4] = modifiedRecord[4].toUpperCase();

        // occurrenceStatus (present vs absent)
        // TODO: confirm there are absence records! Indeed there are records missing individualCount
        if (modifiedRecord[10].isEmpty()) {
          modifiedRecord[16] = Constants.ABSENT;
        } else {
          modifiedRecord[16] =
            TermUtils.getOccurrenceStatus(Integer.valueOf(modifiedRecord[10])).toString().toLowerCase();
        }

        // add static values
        modifiedRecord[17] = "Taiwan"; // country
        modifiedRecord[18] = "TW"; // countryCode

        // static values, based on which nuclear power plant it is: N1 or N2
        if (modifiedRecord[4].equals("N1")) {
          modifiedRecord[19] = "Nuclear Power Plant at Shihmen"; // locality
          modifiedRecord[20] = "25° 17′ 9″ N, 121° 35′ 10″ E"; // verbatimCoordinates
          modifiedRecord[21] = "25.28583"; // decimalLatitude
          modifiedRecord[22] = "121.5861"; // decimalLongitude

        } else {
          modifiedRecord[19] = "Nuclear Power Plant at Yehliu"; // locality
          modifiedRecord[20] = "25° 12′ 10″ N, 121° 39′ 45″ E"; // verbatimCoordinates
          modifiedRecord[21] = "25.20278"; // decimalLatitude
          modifiedRecord[22] = "121.6625"; // decimalLongitude
        }

        modifiedRecord[23] =
          "fish samples were collected monthly from the intake screens at nuclear power plant for 24 h (from 9 AM to 9 AM) on the date chosen by a systematic sampling method (Cochran, W. G. Sampling Techniques. 3rd ed. (John Wiley & Sons, 1977)"; // samplingProtocol
        modifiedRecord[24] = "24"; // sampleSizeValue
        modifiedRecord[25] = "hour"; // sampleSizeUnit
        modifiedRecord[26] = "24hr"; // samplingEffort
        modifiedRecord[27] = "http://creativecommons.org/publicdomain/zero/1.0/legalcode"; // license
        modifiedRecord[28] = "Event"; // type
        modifiedRecord[29] = "Chen H, Liao Y, Chen C, Tsai J, Chen L, Shao K"; // rightsHolder
        modifiedRecord[31] = "MaterialSample"; // basisOfRecord
        modifiedRecord[32] = "Dr. Kwang-Tsao Shao and the senior laboratory members"; // identifiedBy
        modifiedRecord[33] =
          "Identification done using plenty of handbooks of field guide and identification keys."; // identifiedBy
        modifiedRecord[35] = "individuals"; // organismQuantityType
        modifiedRecord[36] = "Animalia"; // kingdom
        modifiedRecord[37] = "Chordata"; // phylum

        // store organismQuantity
        modifiedRecord[34] = modifiedRecord[11]; // same as individualCount

        // construct unique eventID for this sampling period
        // Format: "urn:[institutionID]:[eventDate]:[locationID]"
        // Example: "urn:taibif:1987-08:N2"
        modifiedRecord[0] = "urn:taibif:" + modifiedRecord[3] + ":" + modifiedRecord[4];

        // verify taxonomy
        String name = modifiedRecord[8].trim();

        // for more accurate match, we take higher taxonomy into consideration
        LinneanClassification cl = new NameUsage();
        cl.setFamily(modifiedRecord[6]);
        // only if binomial, set species
        if (name.split(" ").length == 2 && !name.endsWith("spp.")) {
          cl.setSpecies(name);

          // lowest rank specified
          Rank rank = TermUtils.lowestRank(cl);
          if (rank != null) {
            modifiedRecord[43] = rank.toString().toLowerCase();
          }

          // verify name, and add higher taxonomy
          NameUsageMatch match = MATCHING_SERVICE.match(name, rank, cl, false, false);
          if (match.getMatchType().equals(NameUsageMatch.MatchType.EXACT)) {
            modifiedRecord[36] = match.getKingdom();
            modifiedRecord[37] = match.getPhylum();
            modifiedRecord[38] = match.getClazz();
            modifiedRecord[39] = match.getOrder();
            modifiedRecord[40] = match.getFamily();
            modifiedRecord[41] = match.getGenus();
            modifiedRecord[42] = match.getScientificName();
            modifiedRecord[43] = match.getRank().toString();
            modifiedRecord[44] = match.getUsageKey().toString();
            modifiedRecord[45] = match.getStatus().toString();
          } else {
            if (!namesNotFound.contains(name)) {
              LOG.error(
                match.getMatchType().toString() + " match for: " + name + " (with rank " + rank + ") to: " + match
                  .getScientificName() + " (with rank " + match.getRank() + ")"
                + ". See example record with eventDate: " + modifiedRecord[0]);
              namesNotFound.add(name);
            }
          }
        } else {
          namesNotFound.add(name);
        }

        // construct unique occurrenceID for this abundance record:
        // Format: "urn:[institutionCode]:[eventDate]:[locationID]:[taxonID]"
        // Example: "urn:taibif:1994-08:N2:1301"
        modifiedRecord[30] = modifiedRecord[0] + modifiedRecord[44];

        // always output line to new occurrences file
        String row = FileUtils.tabRow(modifiedRecord);
        writerOccs.write(row);

        // only output line to events file if event hasn't been included yet
        String eventID = modifiedRecord[0];
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
    String[] header = new String[46];
    // ***original columns

    // header 0: ID, always empty, so convert to dwc:eventID
    header[0] = "eventID";

    // header 1: year, e.g. 1987
    // maps to dwc:year
    header[1] = "year";

    // header 2: month, e.g. "Mar"
    // maps to dwc:month
    header[2] = "month";

    // header 3: 採集時間 (eventDate), e.g. 1987/03/
    // converted to ISO format 1994-08
    // maps to dwc:eventDate
    header[3] = "eventDate";

    // header 4: Station, e.g. "N2", "N1"
    // maps to dwc:locationID
    header[4] = "locationID";

    // header 5: Sample, sparsely populated
    header[5] = "Sample";

    // header 6: Family, e.g. "Acanthuridae"
    // maps to dwc:family
    header[6] = "family";

    // header 7: 科名 (Family), e.g. "刺尾鯛科"
    header[7] = "family_zh";

    // header 8: TL(cm), e.g. "Naso lituratus"
    // maps to dwc:scientificName
    header[8] = "scientificName";

    // header 9: 中名 (vernacularName in ZH_tw), e.g. "黑背鼻魚"
    // maps to dwc:vernacularName
    header[9] = "vernacularName";

    // header 10: 尾數 (individualCount), e.g. 1
    // maps to dwc:individualCount
    header[10] = "individualCount";

    // header 11: Weight(Tol), e.g. 23
    // TODO: MoF?
    header[11] = "Weight(Tol)";

    // header 12: Weight(Mean), e.g. 66,5
    // TODO: MoF?
    header[12] = "Weight(Mean)";

    // header 13: New, always empty
    header[13] = "New";

    // header 14: Note, e.g. "核二廠入水口"
    // maps to dwc:eventRemarks
    // TODO: add "The samples collected up to April 1990 were recorded as presence-absence data only. From September 2000 on, the samples were recorded quantitatively, i.e., the number of fish of each species was recorded."
    header[14] = "eventRemarks";

    // header 15: TL(cm), e.g. 15.5~16
    // TODO: MoF?
    header[15] = "TL(cm)";

    // ***new augmented columns of information

    // present or absent - depending on individualCount (samples collected up to April 1990 were recorded as presence-absence data only)
    header[16] = "occurrenceStatus";

    // Taiwan
    header[17] = "country";

    // TW
    header[18] = "countryCode";

    // Nuclear Power Plant N1 or N2
    header[19] = "locality";

    // 1st Nuclear Power Plant at Shihmen (25° 17′ 9″ N, 121° 35′ 10″ E)  [25.28583°, 121.5861°]
    // 2nd Nuclear Power Plant at Yehliu (25° 12′ 10″ N, 121° 39′ 45″ E)  [25.20278°, 121.6625°]
    header[20] = "verbatimCoordinates";

    // 25.28583 or 25.20278
    header[21] = "decimalLatitude";

    // 121.5861 or 121.6625
    header[22] = "decimalLongitude";

    // fish samples were collected monthly from the intake screens at nuclear power plant for 24 h (from 9 AM to 9 AM) on the date chosen by a systematic sampling method (Cochran, W. G. Sampling Techniques. 3rd ed. (John Wiley & Sons, 1977), except during the maintenance period which is about one month during winter to spring seasons.
    header[23] = "samplingProtocol";

    // time duration in number of collection hours (24)
    header[24] = "sampleSizeValue";

    // hour
    header[25] = "sampleSizeUnit";

    // number of collection hours (24 hours)
    header[26] = "samplingEffort";

    // http://creativecommons.org/publicdomain/zero/1.0/legalcode (since data in Dryad is licensed under CC0 - http://datadryad.org/resource/doi:10.5061/dryad.m777t?show=full)
    header[27] = "license";

    // Event
    header[28] = "type";

    // Chen H, Liao Y, Chen C, Tsai J, Chen L, Shao K
    header[29] = "rightsHolder";

    // unique occurrenceID
    header[30] = "occurrenceID";

    // MaterialSample
    header[31] = "basisOfRecord";

    // Dr. Kwang-Tsao Shao and the senior laboratory members
    header[32] = "identifiedBy";

    // Identification done using plenty of handbooks of field guide and identification keys.
    header[33] = "identificationRemarks";

    // copied from individualCount
    header[34] = "organismQuantity";

    // individuals
    header[35] = "organismQuantityType";

    // taxonomy
    header[36] = "kingdom_gbif";
    header[37] = "phylum_gbif";
    header[38] = "class_gbif";
    header[39] = "order_gbif";
    header[40] = "family_gbif";
    header[41] = "genus_gbif";
    header[42] = "scientificName_gbif";
    header[43] = "taxonRank";
    header[44] = "taxonID_gbif";
    header[45] = "taxonomicStatus_gbif";

    return header;
  }

}
