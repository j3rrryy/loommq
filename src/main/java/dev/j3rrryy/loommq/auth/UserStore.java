package dev.j3rrryy.loommq.auth;

import java.util.Optional;

public interface UserStore {

    Optional<User> authenticate(String username, String password);

}
