package org.gbif.refine.datasets.hamaarag;

import org.gbif.api.model.checklistbank.NameUsageMatch;
import org.gbif.api.service.checklistbank.NameUsageMatchingService;
import org.gbif.api.vocabulary.Rank;
import org.gbif.io.CSVReader;
import org.gbif.io.CSVReaderFactory;
import org.gbif.refine.client.WebserviceClientModule;
import org.gbif.refine.utils.FileUtils;
import org.gbif.utils.file.ClosableReportingIterator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.util.Arrays;
import java.util.Set;
import javax.validation.constraints.NotNull;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to validate scientific names from the Large mammals in Israel from camera traps dataset
 * published by Hamaarag hosted here: http://cloud.gbif.org/eubon/resource?r=camera and indexed here:
 * https://www.gbif.org/dataset/cdae52c7-a6f2-460e-b0e9-33c795a34224 The publisher hasn't communicated a desire to
 * republish this dataset again. Ideally it would have images added to it, however, and be updated and republished
 * on the Israel GBIF IPT (not operational yet as of November 2017).
 */
public class CameraTraps {

  private static final Logger LOG = LoggerFactory.getLogger(CameraTraps.class);
  private static final NameUsageMatchingService MATCHING_SERVICE =
    WebserviceClientModule.webserviceClientReadOnly().getInstance(NameUsageMatchingService.class);
  private static final String MISAPPLIED = "misapplied";

  public static void main(String[] args) throws IOException {
    // directory where file should be written to
    File output = org.gbif.utils.file.FileUtils.createTempDir();
    processCameraTraps(output);
    LOG.info("Processing complete! occurrences.tab written to: " + output.getAbsolutePath());
  }

  /**
   * Iterates over original observations source file where each observation corresponds to a picture or a short sequence
   * of pictures where a mammal has been observed.
   * i) validates it (e.g. matching scientific names to GBIF Backbone Taxonomy)
   * ii) augments it (e.g. adds new columns for higher taxonomy, etc)
   *
   * @param output directory to write file to
   *
   * @throws IOException if method fails
   */
  public static void processCameraTraps(File output) throws IOException {
    // load the original source file to process
    InputStream fis = CameraTraps.class.getResourceAsStream("/datasets/hamaarag/observations_large_mammals-1.csv");

    // create an iterator on the file
    CSVReader reader = CSVReaderFactory.build(fis, "Latin1", ",", '"', 1);

    // get header row for the new event and occurrence files that this method will output
    String[] header = getHeader();

    // observations file
    Writer writerOccs = FileUtils.startOccurrencesFile(output, header);

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

        String[] modifiedRecord = Arrays.copyOf(record, header.length);

        // add higher taxonomy
        String name = modifiedRecord[18];
        // use rank in search, if it was specified
        Rank r = Rank.SPECIES;
        if (!Strings.isNullOrEmpty(modifiedRecord[19]) && !modifiedRecord[19].equals("null")) {
          r = Rank.valueOf(modifiedRecord[19].toUpperCase());
        }

        NameUsageMatch match = MATCHING_SERVICE.match(name, r, null, false, false);
        if (match.getMatchType().equals(NameUsageMatch.MatchType.EXACT)) {
          modifiedRecord[31] = match.getKingdom();
          modifiedRecord[32] = match.getPhylum();
          modifiedRecord[33] = match.getClazz();
          modifiedRecord[34] = match.getOrder();
          modifiedRecord[35] = match.getFamily();
          modifiedRecord[36] = match.getGenus();

          // specificEpithet
          if (match.getSpecies() != null) {
            String[] parts = match.getSpecies().split(" ");
            if (parts.length == 2) {
              modifiedRecord[37] = parts[1];
            }
          }

          modifiedRecord[38] = match.getScientificName();
          modifiedRecord[39] = match.getStatus().toString().toLowerCase();
        }
        // flag misapplied names
        else {
          modifiedRecord[39] = MISAPPLIED;
          if (!nonMatchingNames.contains(name)) {
            LOG.error(match.getMatchType().toString() + " match for: " + name + " with rank: " + r.toString()
                      + " [scientificName=" + match.getScientificName() + ", rank= " + match.getRank() + "]");
            nonMatchingNames.add(name);
          }
        }

        // always output line to new occurrences file
        String row = FileUtils.tabRow(modifiedRecord);
        writerOccs.write(row);
      }
      LOG.info("Iterated over " + line + " rows.");
      LOG.error("Found " + nonMatchingNames.size() + " non-matching names.");
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
   * @return array of column names in output file. The first ten columns represent Hamaarag's sampling scheme.
   */
  @NotNull
  private static String[] getHeader() {
    String[] header = new String[40];

    header[0] = "campaign";
    header[1] = "group";
    header[2] = "year";
    header[3] = "unit";
    header[4] = "subunit";
    header[5] = "site";
    header[6] = "factor";
    header[7] = "proximity";
    header[8] = "habitat";
    header[9] = "decimalLongitude";
    header[10] = "decimalLatitude";
    header[11] = "eventDate";
    header[12] = "recordedBy";
    header[13] = "event_id";
    header[14] = "date";
    header[15] = "datetime";
    header[16] = "camera_type";
    header[17] = "camera_id";
    header[18] = "michael_scientificName";
    header[19] = "taxonRank";
    header[20] = "infraspecificEpithet";
    header[21] = "taxonRemarks";
    header[22] = "vernacularName";
    header[23] = "occurrenceStatus";
    header[24] = "individualCount";
    header[25] = "young";
    header[26] = "adult";
    header[27] = "dataGeneralizations";
    header[28] = "geodeticDatum";
    header[29] = "locationID";
    header[30] = "occurrenceID";

    // taxonomy
    header[31] = "kingdom";
    header[32] = "phylum";
    header[33] = "class";
    header[34] = "order";
    header[35] = "family";
    header[36] = "genus";
    header[37] = "specificEpithet";
    header[38] = "scientificName";
    header[39] = "taxonomicStatus";

    return header;
  }
}
