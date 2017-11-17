package org.gbif.refine.datasets.nmk;


import org.gbif.api.model.checklistbank.NameUsage;
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
import java.util.Map;
import java.util.Set;
import javax.validation.constraints.NotNull;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used to generate a unique list of taxa from a dataset published by National Museums of Kenya (NMK)
 * and augment it by adding a taxonID column. It also does some quality control, matching each species against the
 * GBIF Backbone Taxonomy. The dataset is hosted here http://ipt.museums.or.ke/ipt/resource?r=nmk2016 and indexed here:
 * https://www.gbif.org/dataset/086d0402-a5e1-42ae-b67f-ac6244178fa2 The publisher hasn't expressed any desire to
 * republish this dataset.
 */
public class RedList {

  private static final Logger LOG = LoggerFactory.getLogger(RedList.class);

  private static final NameUsageMatchingService MATCHING_SERVICE =
    WebserviceClientModule.webserviceClientReadOnly().getInstance(NameUsageMatchingService.class);

  public static void main(String[] args) throws IOException {
    // directory where files should be written to
    File output = org.gbif.utils.file.FileUtils.createTempDir();
    processList(output);
    LOG.info("Processing list complete! taxon.txt and taxaList.txt written to: " + output.getAbsolutePath());
  }

  /**
   * Iterates over original source file and does the following:
   * i) augments it (e.g. adds taxonID)
   * ii) cleans it (e.g. matches name against GBIF Backbone Taxonomy)
   *
   * @param output directory to write files to
   *
   * @throws IOException if method fails
   */
  public static void processList(File output) throws IOException {
    // load the original source file to process
    InputStream fis = RedList.class.getResourceAsStream("/datasets/nmk/OutcomesDatabaseKenya.csv");

    // create an iterator on the file
    CSVReader reader = CSVReaderFactory.build(fis, "UTF-8", ";", '"', 1);

    // get header row for the new event and occurrence files that this method will output
    String[] header = getHeader();

    // taxon file
    File outTaxon = new File(output, "taxon.txt");
    Writer writerTaxon = org.gbif.utils.file.FileUtils.startNewUtf8File(outTaxon);
    writerTaxon.write(FileUtils.tabRow(header));

    // taxa list file
    File outTaxaList = new File(output, "taxaList.txt");
    Writer writerTaxaList = org.gbif.utils.file.FileUtils.startNewUtf8File(outTaxaList);
    String[] taxaListHeader = new String[4];
    taxaListHeader[0] = "taxonID";
    taxaListHeader[1] = "scientificName";
    taxaListHeader[2] = "class";
    taxaListHeader[3] = "family";
    writerTaxaList.write(FileUtils.tabRow(taxaListHeader));

    // to capture all unique taxonIDs
    Map<String, String> taxa = Maps.newHashMap();
    Set<String> nonMatchingNames = Sets.newHashSet();

    ClosableReportingIterator<String[]> iter = null;
    int line = 0;
    try {
      iter = reader.iterator();
      while (iter.hasNext()) {
        line++;
        String[] record = iter.next();
        // to capture taxa
        String[] t = new String[4];

        // create new augmented record
        String[] modifiedRecord = Arrays.copyOf(record, header.length);
        if (Strings.isNullOrEmpty(modifiedRecord[10])) {
          LOG.warn("Skipping line with empty name: " + line);
          continue;
        }

        // scientificName
        modifiedRecord[10] = StringUtils.capitalize(modifiedRecord[10].toLowerCase().trim());
        t[1] = modifiedRecord[10];

        // class
        if (!Strings.isNullOrEmpty(modifiedRecord[12])) {
          modifiedRecord[12] = StringUtils.capitalize(modifiedRecord[12].toLowerCase().trim());
          t[2] = modifiedRecord[12];
        }

        // family
        if (!Strings.isNullOrEmpty(modifiedRecord[13])) {
          modifiedRecord[13] = StringUtils.capitalize(modifiedRecord[13].toLowerCase().trim());
          t[3] = modifiedRecord[13];
        }

        // combination of name+class+family = unique taxon
        String taxon = modifiedRecord[10].replace(" ", "") + modifiedRecord[12] + modifiedRecord[13];

        // construct new taxonID using [institutionID:datasetID:sequence_id]
        String taxonID = Strings.nullToEmpty("NMK:ODK:" + String.valueOf(taxa.size() + 1));

        // reuse existing taxonID for name if possible
        if (taxa.containsKey(taxon)) {
          taxonID = taxa.get(taxon);
        } else {
          taxa.put(taxon, taxonID);

          // always output line to taxa list file
          t[0] = taxonID;
          String taxaRow = FileUtils.tabRow(t);
          writerTaxaList.write(taxaRow);

          // validate name against GBIF Backbone Taxonomy
          NameUsage nameUsage = new NameUsage();
          nameUsage.setTaxonID(taxonID);
          nameUsage.setRank(Rank.SPECIES);
          nameUsage.setScientificName(modifiedRecord[10]);
          nameUsage.setClazz(modifiedRecord[12]);
          nameUsage.setFamily(modifiedRecord[13]);

          NameUsageMatch match = MATCHING_SERVICE.match(modifiedRecord[10], Rank.SPECIES, nameUsage, false, false);
          if (!match.getMatchType().equals(NameUsageMatch.MatchType.EXACT)) {
            if (!nonMatchingNames.contains(modifiedRecord[10])) {
              nonMatchingNames.add(modifiedRecord[10]);
              LOG.error("No exact match for: " + modifiedRecord[10]);
            }
          }
        }

        // taxonID
        modifiedRecord[0] = taxonID;

        // always output line to taxon file
        String row = FileUtils.tabRow(modifiedRecord);
        writerTaxon.write(row);
      }
      LOG.info("Iterated over " + line + " rows.");
      LOG.info("Found " + taxa.size() + " unique taxa.");
      LOG.info("Found " + nonMatchingNames.size() + " non matching names!");
    }  catch (Exception e) {
      // some error validating this file, report
      LOG.error("Exception caught while iterating over file", e);
    } finally {
      if (iter != null) {
        iter.close();
      }
      reader.close();
      writerTaxon.close();
      writerTaxaList.close();
    }
  }

  /**
   * @return array of column names in output file: taxon.txt
   */
  @NotNull
  private static String[] getHeader() {
    String[] header = new String[50];

    header[0] = "taxonID";
    header[1] = "Site Name";
    header[2] = "Polygon present (Yes/No)";
    header[3] = "Country";
    header[4] = "Latitude*";
    header[5] = "Longitude*";
    header[6] = "Area (ha)";
    header[7] = "Elevation maximum (m)";
    header[8] = "Elevation minimum(m)";
    header[9] = "Habitats";
    header[10] = "Globally Threatened Species";
    header[11] = "Common Name";
    header[12] = "Class";
    header[13] = "Family";
    header[14] = "Status 2002";
    header[15] = "Status 2005";
    header[16] = "Status 2006";
    header[17] = "Status 2007";
    header[18] = "Status 2008";
    header[19] = "Status 2009";
    header[20] = "status 2010";
    header[21] = "status 2011";
    header[22] = "staus 2012";
    header[23] = "";
    header[24] = "Range Description";
    header[25] = "IBA";
    header[26] = "Taxonomic Group";
    header[27] = "Taxonomic Group combined";
    header[28] = "Range Restricted Species";
    header[29] = "Globally Significant Congregations";
    header[30] = "References/Literature";
    header[31] = "Contacts";
    header[32] = "Notes";
    header[33] = "Threats";

    return header;
  }
}
