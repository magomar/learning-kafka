package org.learning.kafka;

import io.github.cdimascio.dotenv.Dotenv;
import io.github.cdimascio.dotenv.DotenvEntry;
import org.apache.commons.configuration2.CombinedConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.combined.CombinedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class ConfigurationLoader {
    private static final Logger logger = LoggerFactory.getLogger(ConfigurationLoader.class);
    private static ConfigurationLoader instance;
    private CombinedConfiguration configuration;

    private ConfigurationLoader() {
        Dotenv dotenv = null;
        try {
            dotenv = Dotenv.load();
        } catch (Exception ex) {
            logger.warn("No .env file found");
        }
        Parameters params = new Parameters();
        String configFile = "config.xml";
        CombinedConfigurationBuilder builder = new CombinedConfigurationBuilder()
                .configure(params.fileBased().setFile(new File(configFile)));
        try {
            configuration = builder.getConfiguration();
        } catch (ConfigurationException e) {
            logger.error("Error obtaining configuration from {}", configFile);
            e.printStackTrace();
        }
        if (null != dotenv)
            for (DotenvEntry entry : dotenv.entries()) {
                configuration.setProperty(entry.getKey(), entry.getValue());
            }
    }

    public static synchronized Configuration load() {
        if (null == instance) {
            instance = new ConfigurationLoader();
        }
        return instance.configuration;
    }
}
