package org.gbif.refine.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

/**
 * Constants used across classes.
 */
public class Constants {

  public static final DateFormat ISO_DF = new SimpleDateFormat("yyyy-MM-dd");
  public static final DateFormat ISO_DF_SHORT = new SimpleDateFormat("yyyy-MM");
  public static final String PRESENT = "present";
  public static final String ABSENT = "absent";
  public static final String MISAPPLIED = "misapplied";
}
