package dev.j3rrryy.loommq.auth;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.password4j.Argon2Function;
import com.password4j.Password;
import com.password4j.types.Argon2;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.core.JacksonException;
import tools.jackson.dataformat.yaml.YAMLMapper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public final class YamlUserStore implements UserStore {

    private static final String ARGON2_PREFIX = "$argon2";
    private static final int MEMORY_KB = 19456;
    private static final int ITERATIONS = 2;
    private static final int PARALLELISM = 1;
    private static final int HASH_LENGTH = 32;
    private static final int SALT_LENGTH = 16;

    private static final Argon2Function ARGON2 = Argon2Function.getInstance(
            MEMORY_KB, ITERATIONS, PARALLELISM, HASH_LENGTH, Argon2.ID);

    private static final YAMLMapper YAML = YAMLMapper.builder().build();

    private final Map<String, User> users;

    public YamlUserStore(Path path) {
        this.users = load(path);
        log.info("Loaded {} user(s) from {}", users.size(), path);
    }

    private static Map<String, User> load(Path path) {
        if (!Files.exists(path)) {
            throw new IllegalStateException("Users file not found: " + path);
        }
        try {
            UsersFile file = YAML.readValue(path.toFile(), UsersFile.class);
            if (file.users() == null || file.users().isEmpty()) {
                throw new IllegalStateException("Users file is empty: " + path);
            }
            return file.users().stream()
                    .map(YamlUserStore::toUser)
                    .collect(Collectors.toUnmodifiableMap(User::username, u -> u));
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to load users from " + path, e);
        }
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
            return Optional.empty();
        }
        Argon2Function function = Argon2Function.getInstanceFromHash(user.passwordHash());
        return Password.check(password, user.passwordHash()).with(function)
                ? Optional.of(user)
                : Optional.empty();
    }

    private record UsersFile(@JsonProperty("users") List<UserEntry> users) {

    }

    private record UserEntry(String username, String password, List<PermissionEntry> permissions) {

    }

    private record PermissionEntry(String action, String topic) {

    }

}
