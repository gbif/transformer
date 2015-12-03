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
import java.util.Arrays;
import java.util.Set;

import javax.validation.constraints.NotNull;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class used to clean, augment, and transform the original EndangeredPlantsCoverage.csv dataset published by
 * EBD-CSIC into a DwC sample-based, star format with event records with associated occurrences.
 */
public class EndangeredPlants {
  private static final Logger LOG = LoggerFactory.getLogger(EndangeredPlants.class);

  private static final NameUsageMatchingService MATCHING_SERVICE =
    WebserviceClientModule.webserviceClientReadOnly().getInstance(NameUsageMatchingService.class);

  // set of names that are verified as valid, but don't appear in the GBIF Nub
  private static final Set<String> VALID_NAMES = ImmutableSet.of("Anthoxantum aristatum", "Corema album", "Linaria tursica",
    "Centrathus calcitrapae", "Kickxia cirrhosa", "Salix", "Trifolium");

  public static void main(String[] args) throws IOException {
    // directory where files should be written to
    File output = org.gbif.utils.file.FileUtils.createTempDir();
    processEndangeredPlants(output);
    LOG.info("Processing EndangeredPlantsCoverage.csv complete! event.txt and occurrence.txt written to: " + output
      .getAbsolutePath());
  }

  /**
   * Iterates over original source file and does the following:
   * i) cleans it (e.g. matching scientific names to GBIF Backbone Taxonomy)
   * ii) augments it (e.g. adds occurrenceID, higher taxonomy columns, etc)
   * iii) transforms it into star format (two files events.txt list of unique sampling events and occurrence.txt a
   * list of all observations from all sampling events)
   *
   * @param output directory to write files to
   *
   * @throws IOException if method fails
   */
  public static void processEndangeredPlants(File output) throws IOException {
    // load the original source file to process
    InputStream fis = ShoreBirdCensus.class.getResourceAsStream("/datasets/ebd/EndangeredPlantsCoverage.csv");

    // create an iterator on the file
    CSVReader reader = CSVReaderFactory.build(fis, "Latin1", ";", '"', 1);

    // get header row for the new event and occurrence files that this method will output
    String[] header = getHeader();

    // sampling events file
    Writer writerEvents = FileUtils.startEventsFile(output, header);

    // observations file
    Writer writerOccs = FileUtils.startOccurrencesFile(output, header);

    // capture all unique eventIDs
    Set<String> events = Sets.newHashSet();

    ClosableReportingIterator<String[]> iter = null;
    int line = 0;
    Set<String> nonMatchingNames = Sets.newHashSet();
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
        String eventID = modifiedRecord[0];

        // unique occurrenceID (institutionCode:eventID:sequentialID)
        modifiedRecord[13] = "EBD-CSID:" + eventID + ":" + String.valueOf(line);

        // add static values
        modifiedRecord[9] = "braunBlanquetScale";
        modifiedRecord[11] = Constants.PRESENT;
        modifiedRecord[12] = "EBD-CSIC";
        modifiedRecord[23] = "HumanObservation";
        modifiedRecord[26] = "Spain";
        modifiedRecord[27] = "ES";
        modifiedRecord[28] = "Huelva";
        modifiedRecord[29] = "Parque Nacional de Doñana";
        modifiedRecord[33] = "225";
        modifiedRecord[34] = "square_metre";
        modifiedRecord[35] = "square plot";
        modifiedRecord[37] = "Event";
        modifiedRecord[38] = "http://creativecommons.org/licenses/by/4.0/legalcode";
        modifiedRecord[39] = "WGS84";

        // depending on eventID, set location fields
        if (eventID.equalsIgnoreCase("pun20140428")) {
          modifiedRecord[10] = "2014-04-28";
          modifiedRecord[24] = "36.969353";
          modifiedRecord[25] = "-6.445742";
          modifiedRecord[30] = "Pun";
          modifiedRecord[31] = "El Puntal";
          modifiedRecord[32] = "Rocío Fernández Zamudio | Estefanía Becerra Moreno";
          modifiedRecord[36] = "pun-s";
        }

        else if (eventID.equalsIgnoreCase("c3220140428")) {
          modifiedRecord[10] = "2014-04-28";
          modifiedRecord[24] = "36.992254";
          modifiedRecord[25] = "-6.516209";
          modifiedRecord[30] = "C32";
          modifiedRecord[31] = "Cota 32";
          modifiedRecord[32] = "Rocío Fernández Zamudio | Estefanía Becerra Moreno";
          modifiedRecord[36] = "c32-s";
        }

        else if (eventID.equalsIgnoreCase("cal20140513")) {
          modifiedRecord[10] = "2014-05-13";
          modifiedRecord[24] = "37.049771";
          modifiedRecord[25] = "-6.440769";
          modifiedRecord[30] = "Cal";
          modifiedRecord[31] = "Casa de la Algaida";
          modifiedRecord[32] = "Rocío Fernández Zamudio | Estefanía Becerra Moreno";
          modifiedRecord[36] = "cal-s";
        }

        else if (eventID.equalsIgnoreCase("cor20140514")) {
          modifiedRecord[10] = "2014-05-14";
          modifiedRecord[24] = "36.969236";
          modifiedRecord[25] = "-6.445826";
          modifiedRecord[30] = "Cor";
          modifiedRecord[31] = "El corchuelo";
          modifiedRecord[32] = "Rocío Fernández Zamudio";
          modifiedRecord[36] = "cor-s";
        }

        else if (eventID.equalsIgnoreCase("c2d20140521")) {
          modifiedRecord[10] = "2014-05-21";
          modifiedRecord[24] = "37.033725";
          modifiedRecord[25] = "-6.439861";
          modifiedRecord[30] = "C2d";
          modifiedRecord[31] = "Cercado 2 dentro";
          modifiedRecord[32] = "Rocío Fernández Zamudio";
          modifiedRecord[36] = "c2d-s";
        }

        else if (eventID.equalsIgnoreCase("c2f20140521")) {
          modifiedRecord[10] = "2014-05-21";
          modifiedRecord[24] = "37.033606";
          modifiedRecord[25] = "-6.439724";
          modifiedRecord[30] = "C2f";
          modifiedRecord[31] = "Cercado 2 fuera";
          modifiedRecord[32] = "Rocío Fernández Zamudio";
          modifiedRecord[36] = "c2f-s";
        }


        else if (eventID.equalsIgnoreCase("c4d20140527")) {
          modifiedRecord[10] = "2014-05-27";
          modifiedRecord[24] = "37.028892";
          modifiedRecord[25] = "-6.439081";
          modifiedRecord[30] = "C4d";
          modifiedRecord[31] = "Cercado 4 dentro";
          modifiedRecord[32] = "Rocío Fernández Zamudio | Estefanía Becerra Moreno";
          modifiedRecord[36] = "c4d-s";
        }

        else if (eventID.equalsIgnoreCase("c4f20140526")) {
          modifiedRecord[10] = "2014-05-26";
          modifiedRecord[24] = "37.028968";
          modifiedRecord[25] = "-6.438869";
          modifiedRecord[30] = "C4f";
          modifiedRecord[31] = "Cercado 4 fuera";
          modifiedRecord[32] = "Rocío Fernández Zamudio | Estefanía Becerra Moreno";
          modifiedRecord[36] = "c4f-s";
        }

        else if (eventID.equalsIgnoreCase("stg20140611")) {
          modifiedRecord[10] = "2014-06-11";
          modifiedRecord[24] = "37.078263";
          modifiedRecord[25] = "-6.449509";
          modifiedRecord[30] = "Stg";
          modifiedRecord[31] = "Soto Grande";
          modifiedRecord[32] = "Rocío Fernández Zamudio | Manoli Becerra | Estefanía Becerra Moreno";
          modifiedRecord[36] = "stg-s";
        }

        else if (eventID.equalsIgnoreCase("nsn20140617")) {
          modifiedRecord[10] = "2014-06-17";
          modifiedRecord[24] = "37.02414";
          modifiedRecord[25] = "-6.4519";
          modifiedRecord[30] = "Nsn";
          modifiedRecord[31] = "Navazo de la Sarna";
          modifiedRecord[32] = "Rocío Fernández Zamudio | Luis alfonso Ramírez";
          modifiedRecord[36] = "nsn-s";
        }

        else if (eventID.equalsIgnoreCase("sor20140618")) {
          modifiedRecord[10] = "2014-06-18";
          modifiedRecord[24] = "37.051966";
          modifiedRecord[25] = "-6.544465";
          modifiedRecord[30] = "Sor";
          modifiedRecord[31] = "Laguna de la Soriana";
          modifiedRecord[32] = "Rocío Fernández Zamudio | Luis alfonso Ramírez";
          modifiedRecord[36] = "sor-s";
        }

        else if (eventID.equalsIgnoreCase("pun20130402")) {
          modifiedRecord[10] = "2013-04-02";
          modifiedRecord[24] = "36.969353";
          modifiedRecord[25] = "-6.445742";
          modifiedRecord[30] = "Pun";
          modifiedRecord[31] = "El Puntal";
          modifiedRecord[32] = "Rocío Fernández Zamudio";
          modifiedRecord[36] = "pun-s";
        }

        else if (eventID.equalsIgnoreCase("c3220130408")) {
          modifiedRecord[10] = "2013-04-08";
          modifiedRecord[24] = "36.992254";
          modifiedRecord[25] = "-6.516209";
          modifiedRecord[30] = "C32";
          modifiedRecord[31] = "Cota 32";
          modifiedRecord[32] = "Rocío Fernández Zamudio";
          modifiedRecord[36] = "c32-s";
        }

        else if (eventID.equalsIgnoreCase("cor20130524")) {
          modifiedRecord[10] = "2013-05-24";
          modifiedRecord[24] = "36.969236";
          modifiedRecord[25] = "-6.445826";
          modifiedRecord[30] = "Cor";
          modifiedRecord[31] = "El corchuelo";
          modifiedRecord[32] = "Rocío Fernández Zamudio";
          modifiedRecord[36] = "cor-s";
        }

        else if (eventID.equalsIgnoreCase("cal20130605")) {
          modifiedRecord[10] = "2013-06-05";
          modifiedRecord[24] = "37.049771";
          modifiedRecord[25] = "-6.440769";
          modifiedRecord[30] = "Cal";
          modifiedRecord[31] = "Casa de la Algaida";
          modifiedRecord[32] = "Rocío Fernández Zamudio";
          modifiedRecord[36] = "cal-s";
        }

        else if (eventID.equalsIgnoreCase("sor20130705")) {
          modifiedRecord[10] = "2013-06-05";
          modifiedRecord[24] = "37.051966";
          modifiedRecord[25] = "-6.544465";
          modifiedRecord[30] = "Sor";
          modifiedRecord[31] = "Laguna de la Soriana";
          modifiedRecord[32] = "Rocío Fernández Zamudio";
          modifiedRecord[36] = "sor-s";
        }

        else if (eventID.equalsIgnoreCase("nsn20130715")) {
          modifiedRecord[10] = "2013-07-15";
          modifiedRecord[24] = "37.02414";
          modifiedRecord[25] = "-6.4519";
          modifiedRecord[30] = "Nsn";
          modifiedRecord[31] = "Navazo de la Sarna";
          modifiedRecord[32] = "Rocío Fernández Zamudio";
          modifiedRecord[36] = "nsn-s";
        } else {
          LOG.error("Line " + line + " has no recognizable eventID!!");
        }

        // add higher taxonomy
        String name = modifiedRecord[3];

        // rank if it was specified, defaulting to SPECIES
        Rank r = Rank.SPECIES;
        if (!Strings.isNullOrEmpty(modifiedRecord[4]) && !modifiedRecord[4].equals("null")) {
          r = Rank.valueOf(modifiedRecord[4].toUpperCase());
        }

        NameUsageMatch match = MATCHING_SERVICE.match(name, r, null, false, false);
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
          modifiedRecord[4] = match.getRank().toString().toLowerCase();
          modifiedRecord[22] = match.getStatus().toString().toLowerCase();
        }
        // write valid names that don't appear in GBIF Nub
        else if (VALID_NAMES.contains(name)) {
          modifiedRecord[21] = name;
          modifiedRecord[4] = r.toString().toLowerCase();

          // make sure to write genus, specificEpithet
          String[] parts = name.split(" ");
          if (parts.length == 2) {
            modifiedRecord[19] = parts[0];
            modifiedRecord[20] = parts[1];
          }
        }
        // for bare ground, indicate there was the absence of plantae
        else if (Strings.isNullOrEmpty(name)) {
          modifiedRecord[14] = "Plantae";
          modifiedRecord[11] = Constants.ABSENT;
        }
        // flag misapplied names
        else {
          modifiedRecord[21] = name;
          modifiedRecord[22] = Constants.MISAPPLIED;
          if (!nonMatchingNames.contains(name)) {
            LOG.error(match.getMatchType().toString() + " match for: "+ name + " with rank: " + r.toString() + " [scientificName=" + match.getScientificName() + ", rank= " + match.getRank() + "]" + " , occurrenceID: " + modifiedRecord[13]);
            nonMatchingNames.add(name);
          }
        }

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
   * @return array of column names in output files (event.txt and occurrence.txt)
   */
  @NotNull
  private static String[] getHeader() {
    String[] header = new String[40];
    header[0] = "eventID";
    header[1] = "ESTRATO";
    header[2] = "ESPECIE (ORIGINAL)";
    header[3] = "ESPECIE (ACTUALIZADO)";
    header[4] = "taxonRank";
    header[5] = "occurrenceRemarks";
    header[6] = "organismQuantity";
    header[7] = "ABUNDANCIA_AMENAZADAS";
    header[8] = "INDIVIDUOS";
    header[9] = "organismQuantityType";

    // mandatory
    header[10] = "eventDate";
    header[11] = "occurrenceStatus";
    header[12] = "institutionCode";
    header[13] = "occurrenceID";

    // taxonomy
    header[14] = "kingdom";
    header[15] = "phylum";
    header[16] = "class";
    header[17] = "order";
    header[18] = "family";
    header[19] = "genus";
    header[20] = "specificEpithet";
    header[21] = "scientificName";
    header[22] = "taxonomicStatus";

    header[23] = "basisOfRecord";

    // location
    header[24] = "decimalLatitude";
    header[25] = "decimalLongitude";
    header[26] = "country";
    header[27] = "countryCode";
    header[28] = "stateProvince";
    header[29] = "locality";
    header[30] = "locationID";
    header[31] = "verbatimLocality";

    // other
    header[32] = "recordedBy";
    header[33] = "sampleSizeValue";
    header[34] = "sampleSizeUnit";
    header[35] = "samplingProtocol";
    header[36] = "parentEventID";
    header[37] = "type";
    header[38] = "license";
    header[39] = "geodeticDatum";

    return header;
  }
}
