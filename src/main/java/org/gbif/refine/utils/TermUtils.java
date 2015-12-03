package org.gbif.refine.utils;

import org.gbif.api.vocabulary.OccurrenceStatus;

import javax.validation.constraints.NotNull;


/**
 * Set of utilities used to populate (DC, DwC, etc) Terms.
 */
public class TermUtils {

  public TermUtils() {
  }

  /**
   * Return the occurrenceStatus for an occurrence given its individualCount.
   *
   * @param individualCount individualCount
   *
   * @return PRESENT or ABSENT
   */
  @NotNull
  public static OccurrenceStatus getOccurrenceStatus(int individualCount) {
    return (individualCount > 0) ? OccurrenceStatus.PRESENT : OccurrenceStatus.ABSENT;
  }
}
