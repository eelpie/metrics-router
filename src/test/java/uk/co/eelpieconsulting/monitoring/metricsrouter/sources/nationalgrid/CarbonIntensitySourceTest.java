package uk.co.eelpieconsulting.monitoring.metricsrouter.sources.nationalgrid;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CarbonIntensitySourceTest {

  @Test
  public void canParseJson() throws Exception {
    String json = readFile("intensity.json");

    String forecast = new CarbonIntensitySource().parseJson(json).get("carbonintensity.forecast");
    String actual = new CarbonIntensitySource().parseJson(json).get("carbonintensity.actual");

    assertEquals("279", forecast);
    assertNull(actual);
  }

  @Test
  public void shouldIncludeActualIfProvided() throws Exception {
    String json = readFile("intensity-actual.json");

    String actual = new CarbonIntensitySource().parseJson(json).get("carbonintensity.actual");

    assertEquals("311", actual);
  }

  private String readFile(String filename) throws IOException {
    String path = getClass().getClassLoader().getResource(filename).getPath();
    return IOUtils.toString(new FileInputStream(new File(path)), "UTF-8");
  }

}
