package org.gbif.refine.utils;

import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.StringUtils;

/**
 * Set of utilities used to operate on files.
 */
public class FileUtils {

  private static final Pattern escapeChars = Pattern.compile("[\t\n\r]");

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
}
