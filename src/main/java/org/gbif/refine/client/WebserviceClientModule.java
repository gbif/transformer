package org.gbif.refine.client;

import org.gbif.checklistbank.ws.client.guice.ChecklistBankWsClientModule;
import org.gbif.ws.client.guice.AnonymousAuthModule;

import java.io.IOException;
import java.util.Properties;

import com.google.common.base.Throwables;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;

/**
 * Class makes webservice clients available for use.
 */
public class WebserviceClientModule {

  private static Injector webserviceClientReadOnly;
  private static Properties clbProperties;

  private WebserviceClientModule() {
  }

  /**
   * Load the Properties needed to configure the clb webservice client from the clb.properties file.
   */
  @Singleton
  public static synchronized Properties clbProperties() {
    if (clbProperties == null) {
      Properties p = new Properties();
      try {
        p.load(WebserviceClientModule.class.getResourceAsStream("/config/clb.properties"));
      } catch (IOException e) {
        throw Throwables.propagate(e);
      } finally {
        clbProperties = p;
      }
    }
    return clbProperties;
  }

  /**
   * @return An injector that is bound for the webservice client layer but read-only.
   */
  public static synchronized Injector webserviceClientReadOnly() {
    if (webserviceClientReadOnly == null) {
      // Anonymous authentication module used, webservice client will be read-only
      webserviceClientReadOnly =
        Guice.createInjector(new AnonymousAuthModule(), new ChecklistBankWsClientModule(clbProperties(), true, true));
    }
    return webserviceClientReadOnly;
  }
}
