package dev.j3rrryy.loommq.config;

import tools.jackson.core.JacksonException;
import tools.jackson.dataformat.yaml.YAMLMapper;

import java.nio.file.Files;
import java.nio.file.Path;

public final class ConfigLoader {

    private static final YAMLMapper YAML = YAMLMapper.builder().build();

    private static Config mergeWithDefaults(Config parsed) {
        Config defaults = Config.defaults();
        return new Config(
                parsed.host() != null ? parsed.host() : defaults.host(),
                parsed.port() != 0 ? parsed.port() : defaults.port(),
                parsed.usersFile() != null ? parsed.usersFile() : defaults.usersFile()
        );
    }

    public Config load(Path configPath) {
        if (configPath == null || !Files.exists(configPath)) {
            return Config.defaults();
        }
        try {
            return mergeWithDefaults(YAML.readValue(configPath.toFile(), Config.class));
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to load config from " + configPath, e);
        }
    }

}
