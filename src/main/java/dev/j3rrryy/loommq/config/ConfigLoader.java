package dev.j3rrryy.loommq.config;

import java.nio.file.Files;
import java.nio.file.Path;

import tools.jackson.dataformat.yaml.YAMLMapper;

public final class ConfigLoader {

    private static final YAMLMapper YAML = YAMLMapper.builder().build();

    private static Config mergeWithDefaults(Config parsed) {
        Config defaults = Config.defaults();
        return new Config(
                parsed.host() != null ? parsed.host() : defaults.host(),
                parsed.port() != 0 ? parsed.port() : defaults.port(),
                parsed.metricsPort() != 0 ? parsed.metricsPort() : defaults.metricsPort(),
                parsed.usersFile() != null ? parsed.usersFile() : defaults.usersFile(),
                parsed.walFile() != null ? parsed.walFile() : defaults.walFile(),
                parsed.rateLimit() != null ? parsed.rateLimit() : defaults.rateLimit()
        );
    }

    public Config load(Path configPath) {
        if (configPath == null || !Files.isRegularFile(configPath)) {
            return Config.defaults();
        }
        try {
            if (Files.size(configPath) == 0) {
                return Config.defaults();
            }
            return mergeWithDefaults(YAML.readValue(configPath.toFile(), Config.class));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load config from " + configPath, e);
        }
    }

}
