package org.gbif.refine.datasets.ebd;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.vocabulary.Rank;
import org.gbif.io.CSVReader;
import org.gbif.io.CSVReaderFactory;
import org.gbif.refine.client.WebserviceClientModule;
import org.gbif.refine.utils.Constants;
import org.gbif.refine.utils.FileUtils;
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

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class used to clean, augment, and transform the original ShoreBirdCensus_1993.csv dataset published by EBD-CSIC
 * into a DwC sample-based, star format with event records with associated occurrences.
 */
public class ShoreBirdCensus {
  private static final Logger LOG = LoggerFactory.getLogger(ShoreBirdCensus.class);

  private static final NameUsageMatchingService MATCHING_SERVICE =
    WebserviceClientModule.webserviceClientReadOnly().getInstance(NameUsageMatchingService.class);

  public static void main(String[] args) throws IOException {
    // directory where files should be written to
    File output = org.gbif.utils.file.FileUtils.createTempDir();
    processShoreBirdCensus(output);
    LOG.info("Processing complete! event.txt and occurrence.txt written to: " + output.getAbsolutePath());
  }

  /**
   * Iterates over original source file and does the following:
   * i) cleans it (e.g. converting dates to ISO format, matching scientific names to GBIF Backbone Taxonomy)
   * ii) augments it (e.g. adds occurrenceID, higher taxonomy columns, etc)
   * iii) transforms it into star format (two files events.txt list of unique sampling events and occurrence.txt a
   * list of all observations from all sampling events)
   *
   * @param output directory to write files to
   *
   * @throws IOException if method fails
   */
  public static void processShoreBirdCensus(File output) throws IOException {
    // load the original source file to process
    InputStream fis = ShoreBirdCensus.class.getResourceAsStream("/datasets/ebd/ShoreBirdCensus_1993.csv");

    // create an iterator on the file
    CSVReader reader = CSVReaderFactory.build(fis, "Latin1", ";", '"', 1);

    // get header row for the new event and occurrence files that this method will output
    String[] header = getHeader();

    // sampling events file
    File outEvents = new File(output, "events.tab");
    Writer writerEvents = org.gbif.utils.file.FileUtils.startNewUtf8File(outEvents);
    // write header
    writerEvents.write(FileUtils.tabRow(header));

    // observations file
    File outOccs = new File(output, "occurrences.tab");
    Writer writerOccs = org.gbif.utils.file.FileUtils.startNewUtf8File(outOccs);
    // write header
    writerOccs.write(FileUtils.tabRow(header));

    // to capture all unique eventIDs
    Set<String> events = Sets.newHashSet();

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

        // unique eventID
        String eventID = Strings.nullToEmpty(modifiedRecord[0]);

        // unique occurrenceID (institutionCode:datasetID:sequentialID)
        modifiedRecord[25] = "EBD-CSIC:SP_SI001346_ShoreBirdCensus:" + eventID + ":" + String.valueOf(line);

        // add static values
        modifiedRecord[24] = "EBD-CSIC";
        modifiedRecord[26] = "http://creativecommons.org/licenses/by/4.0/legalcode";
        modifiedRecord[27] = "Event";
        modifiedRecord[28] = "HumanObservation";
        modifiedRecord[38] = "square_kilometre";
        modifiedRecord[30] = "individuals";
        modifiedRecord[31] = "es";
        modifiedRecord[32] = "SP_SI001346_ShoreBirdCensus";
        modifiedRecord[33] = "0";
        modifiedRecord[34] = "5";
        modifiedRecord[35] = "Band census";
        modifiedRecord[39] = "Spain";
        modifiedRecord[40] = "ES";
        modifiedRecord[41] = "Huelva";
        modifiedRecord[42] = "Parque Nacional de Doñana";

        // convert date (column #1) into ISO format
        String d = modifiedRecord[1];
        DateFormat df = new SimpleDateFormat("dd-MMM-yy", new Locale("es", "ES"));
        Date date = df.parse(d);
        modifiedRecord[1] = Constants.ISO_DF.format(date);

        // create eventTime (column #11)
        String startTime = record[6];
        String endTime = record[7];
        if (!Strings.isNullOrEmpty(startTime) && !Strings.isNullOrEmpty(endTime) && !startTime.equals("-") && !endTime
          .equals("-")) {
          modifiedRecord[6] = startTime + "+01";
          modifiedRecord[7] = endTime + "+01";
          modifiedRecord[12] = startTime + "+01/" + endTime + "+01";
        }

        // create dynamicProperties (column #12)
        JSONObject jo = new JSONObject();
        if (!Strings.isNullOrEmpty(modifiedRecord[8]) && !modifiedRecord[8].equals("-")) {
          jo.put("cloudiness", modifiedRecord[8]);
        }
        if (!Strings.isNullOrEmpty(modifiedRecord[9]) && !modifiedRecord[9].equals("-")) {
          jo.put("wind speed", modifiedRecord[9]);
        }
        if (!Strings.isNullOrEmpty(modifiedRecord[10]) && !modifiedRecord[10].equals("-")) {
          jo.put("wind direction", modifiedRecord[10]);
        }
        if (!Strings.isNullOrEmpty(modifiedRecord[11]) && !modifiedRecord[11].equals("-")) {
          jo.put("waves", modifiedRecord[11]);
        }
        if (jo.toString().length() > 5) {
          modifiedRecord[13] = jo.toString();
        }

        // add higher taxonomy
        String name = modifiedRecord[3];
        NameUsageMatch match = MATCHING_SERVICE.match(name, Rank.SPECIES, null, false, false);
        if (match.getMatchType().equals(NameUsageMatch.MatchType.EXACT)) {
          modifiedRecord[14] = match.getKingdom();
          modifiedRecord[15] = match.getPhylum();
          modifiedRecord[16] = match.getClazz();
          modifiedRecord[17] = match.getOrder();
          modifiedRecord[18] = match.getFamily();
          modifiedRecord[19] = match.getGenus();

          // specificEpithet
          if (match.getSpecies() != null) {
            String[] parts = match.getSpecies().split(" ");
            if (parts.length == 2) {
              modifiedRecord[20] = parts[1];
            }
          }

          modifiedRecord[21] = match.getScientificName();
          modifiedRecord[22] = "species";
          modifiedRecord[23] = match.getStatus().toString();
        } else if (name.equals("Sterna sp.")) {
          LOG.error("Handling special case for Sterna sp.");
          modifiedRecord[19] = "Sterna";
          modifiedRecord[21] = "Sterna sp.";
        } else {
          LOG.error("No exact match for: "+ name);
        }

        // depending on locationID, set WKT representation and sampleSizeValue of area being sampled
        // also create one sampling event per location sampled
        String locationID = modifiedRecord[2];
        if (locationID.equalsIgnoreCase("cama")) {
          modifiedRecord[29] = "POLYGON((-6.529167 36.981904, -6.470398 36.981904, -6.470398 36.931576, -6.529167 36.931576, -6.529167 36.981904))";
          modifiedRecord[37] = "4";
          modifiedRecord[0] = modifiedRecord[0] + "-CAMA";
        } else if (locationID.equalsIgnoreCase("zaca")) {
          modifiedRecord[29] = "POLYGON((-6.470398 36.931576, -6.429026 36.931576, -6.429026 36.873193, -6.470398 36.873193, -6.470398 36.931576))";
          modifiedRecord[37] = "2.1";
          modifiedRecord[0] = modifiedRecord[0] + "-ZACA";
        } else if (locationID.equalsIgnoreCase("maza")) {
          modifiedRecord[29] = "POLYGON((-6.429026 36.873193, -6.346964 36.873193, -6.346964 36.804956, -6.429026 36.804956, -6.429026 36.873193))";
          modifiedRecord[37] = "2.4";
          modifiedRecord[0] = modifiedRecord[0] + "-MAZA";
        } else {
          LOG.error("Line " + line + " has no location!!");
        }

        // occurrenceStatus (present vs absent)
        modifiedRecord[36] = (Integer.valueOf(modifiedRecord[4]) > 0) ? Constants.PRESENT : Constants.ABSENT;

        // always output line to new occurrences file
        String row = FileUtils.tabRow(modifiedRecord);
        writerOccs.write(row);

        // only output line to events file if event hasn't been included yet
        if (!events.contains(eventID)) {
          writerEvents.write(row);
          events.add(eventID);
        }
      }
      LOG.info("Iterated over " + line + " rows.");
      LOG.info("Found " + events.size() + " unique events.");
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
    String[] header = new String[43];
    header[0] = "eventID";
    header[1] = "eventDate";
    header[2] = "locationID";
    header[3] = "name";
    header[4] = "organismQuantity";
    header[5] = "occurrenceRemarks";

    // combined into eventTime - column 12
    header[6] = "startTime";
    header[7] = "endTime";

    // combined into dynamicProperties - column 13
    header[8] = "cloudiness";
    header[9] = "wind speed";
    header[10] = "wind direction";
    header[11] = "waves";

    header[12] = "eventTime";
    header[13] = "dynamicProperties";

    // higher taxonomy
    header[14] = "kingdom";
    header[15] = "phylum";
    header[16] = "class";
    header[17] = "order";
    header[18] = "family";
    header[19] = "genus";
    header[20] = "specificEpithet";
    header[21] = "scientificName";
    header[22] = "taxonRank";
    header[23] = "taxonomicStatus";

    // other
    header[24] = "institutionCode";
    header[25] = "occurrenceID";
    header[26] = "license";
    header[27] = "type";
    header[28] = "basisOfRecord";
    header[29] = "footprintWKT";
    header[30] = "organismQuantityType";
    header[31] = "language";
    header[32] = "datasetID";
    header[33] = "minimumElevationInMeters";
    header[34] = "maximumElevationInMeters";
    header[35] = "samplingProtocol";
    header[36] = "occurrenceStatus";
    header[37] = "sampleSizeValue";
    header[38] = "sampleSizeUnit";
    header[39] = "country";
    header[40] = "countryCode";
    header[41] = "stateProvince";
    header[42] = "locality";

    return header;
  }
}