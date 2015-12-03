package org.gbif.refine.utils;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;

/**
 * Set of utilities used to operate on files.
 */
public class FileUtils {

  private static final Pattern escapeChars = Pattern.compile("[\t\n\r]");
  private static String eventsFileName = "events.tab";
  private static String occurrencesFileName = "occurrences.tab";

  public FileUtils() {
  }

  /**
   * Generate a row/string of values tab delimited. Line breaking characters encountered in
   * a value are replaced with an empty character.
   *
   * @param columns array of values/columns
   *
   * @return row/string of values tab delimited
   */
  @NotNull
  public static String tabRow(String[] columns) {
    // escape \t \n \r chars !!!
    for (int i = 0; i < columns.length; i++) {
      if (columns[i] != null) {
        columns[i] = StringUtils.trimToNull(escapeChars.matcher(columns[i]).replaceAll(" "));
      }
    }
    return StringUtils.join(columns, '\t') + "\n";
  }

  /**
   * Create a new events file in the output directory and write its header line.
   *
   * @param output directory
   * @param header column list equal to header line
   *
   * @return writer on file
   *
   * @throws IOException if writer failed to be created
   */
  public static Writer startEventsFile(File output, String[] header) throws IOException {
    File outEvents = new File(output, eventsFileName);
    Writer writerEvents = org.gbif.utils.file.FileUtils.startNewUtf8File(outEvents);
    writerEvents.write(FileUtils.tabRow(header));
    return writerEvents;
  }

  /**
   * Create a new occurrences file in the output directory and write its header line.
   *
   * @param output directory
   * @param header column list equal to header line
   *
   * @return writer on file
   *
   * @throws IOException if writer failed to be created
   */
  public static Writer startOccurrencesFile(File output, String[] header) throws IOException {
    File outEvents = new File(output, occurrencesFileName);
    Writer writerEvents = org.gbif.utils.file.FileUtils.startNewUtf8File(outEvents);
    writerEvents.write(FileUtils.tabRow(header));
    return writerEvents;
  }
}
