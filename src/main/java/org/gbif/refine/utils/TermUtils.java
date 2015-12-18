package org.gbif.refine.utils;

import org.gbif.api.model.common.LinneanClassification;
import org.gbif.api.vocabulary.OccurrenceStatus;
import org.gbif.api.vocabulary.Rank;

import javax.annotation.Nullable;
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

  /**
   * Return name usage's most specific name (lowest taxonomic rank).
   *
   * @param usage name usage
   *
   * @return lowest rank
   */
  @Nullable
  public static Rank lowestRank(LinneanClassification usage)  {
    if (usage.getSpecies() != null) {
      return Rank.SPECIES;
    } else if (usage.getGenus() != null) {
      return Rank.GENUS;
    } else if (usage.getFamily() != null) {
      return Rank.FAMILY;
    } else if (usage.getOrder() != null) {
      return Rank.ORDER;
    } else if (usage.getClazz() != null) {
      return Rank.CLASS;
    } else if (usage.getPhylum() != null) {
      return Rank.PHYLUM;
    } else if (usage.getKingdom() != null) {
      return Rank.KINGDOM;
    } else {
      return null;
    }
  }
}
