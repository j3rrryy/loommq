package dev.j3rrryy.loommq.config;

import java.nio.file.Path;

public record Config(String host, int port, Path usersFile) {

    public static Config defaults() {
        return new Config("0.0.0.0", 1883, Path.of("users.yaml"));
    }

}
