package dev.j3rrryy.loommq.auth;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.password4j.Argon2Function;
import com.password4j.Password;
import com.password4j.types.Argon2;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.dataformat.yaml.YAMLMapper;

@Slf4j
public final class YamlUserStore implements UserStore {

    private static final String ARGON2_PREFIX = "$argon2";
    private static final int MEMORY_KB = 19456;
    private static final int ITERATIONS = 2;
    private static final int PARALLELISM = 1;
    private static final int HASH_LENGTH = 32;
    private static final int SALT_LENGTH = 16;

    private static final String BOOTSTRAP_USER_ENV = "LOOMMQ_BOOTSTRAP_ADMIN_USERNAME";
    private static final String BOOTSTRAP_PASS_ENV = "LOOMMQ_BOOTSTRAP_ADMIN_PASSWORD";

    private static final Argon2Function ARGON2 = Argon2Function.getInstance(
            MEMORY_KB, ITERATIONS, PARALLELISM, HASH_LENGTH, Argon2.ID);

    private static final YAMLMapper YAML = YAMLMapper.builder().build();

    private final Map<String, User> users;
    private final Counter authFailures;

    public YamlUserStore(Path path, MeterRegistry registry) {
        this.users = load(path);
        this.authFailures = Counter.builder("loommq.auth.failures").register(registry);
        log.info("Loaded {} user(s) from {}", users.size(), path);
    }

    private static Map<String, User> load(Path path) {
        if (!Files.isRegularFile(path)) {
            return bootstrapAdmin();
        }
        try {
            if (Files.size(path) == 0) {
                return bootstrapAdmin();
            }
            UsersFile file = YAML.readValue(path.toFile(), UsersFile.class);
            if (file.users() == null || file.users().isEmpty()) {
                return bootstrapAdmin();
            }
            return file.users().stream()
                    .map(YamlUserStore::toUser)
                    .collect(Collectors.toUnmodifiableMap(User::username, u -> u));
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load users from " + path, e);
        }
    }

    private static Map<String, User> bootstrapAdmin() {
        String password = System.getenv(BOOTSTRAP_PASS_ENV);
        if (password == null || password.isBlank()) {
            throw new IllegalStateException(
                    "Users file is empty and " + BOOTSTRAP_PASS_ENV + " is not set");
        }

        String username = System.getenv(BOOTSTRAP_USER_ENV);
        if (username == null || username.isBlank()) {
            username = "admin";
        }
        log.warn("Bootstrapping '{}' user from environment variables", username);

        String hash = Password.hash(password).addRandomSalt(SALT_LENGTH).with(ARGON2).getResult();
        User admin = new User(username, hash, List.of(new Permission(Permission.Action.ALL, "#")));
        return Map.of(username, admin);
    }

    private static User toUser(UserEntry entry) {
        String hash = entry.password().startsWith(ARGON2_PREFIX)
                ? entry.password()
                : Password.hash(entry.password()).addRandomSalt(SALT_LENGTH).with(ARGON2).getResult();
        List<Permission> permissions = entry.permissions() == null
                ? List.of()
                : entry.permissions().stream()
                .map(p -> new Permission(Permission.Action.parse(p.action()), p.topic()))
                .toList();
        return new User(entry.username(), hash, permissions);
    }

    @Override
    public Optional<User> authenticate(String username, String password) {
        if (username == null || password == null) {
            return Optional.empty();
        }
        User user = users.get(username);
        if (user == null) {
            authFailures.increment();
            return Optional.empty();
        }
        Argon2Function function = Argon2Function.getInstanceFromHash(user.passwordHash());
        if (Password.check(password, user.passwordHash()).with(function)) {
            return Optional.of(user);
        }
        authFailures.increment();
        return Optional.empty();
    }

    private record UsersFile(@JsonProperty("users") List<UserEntry> users) {

    }

    private record UserEntry(
            @JsonProperty("username") String username,
            @JsonProperty("password") String password,
            @JsonProperty("permissions") List<PermissionEntry> permissions) {

    }

    private record PermissionEntry(
            @JsonProperty("action") String action,
            @JsonProperty("topic") String topic) {

    }

}
