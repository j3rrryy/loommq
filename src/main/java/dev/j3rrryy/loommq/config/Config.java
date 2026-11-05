package dev.j3rrryy.loommq.config;

public record Config(String host, int port) {

    public static Config defaults() {
        return new Config("0.0.0.0", 1883);
    }

}
