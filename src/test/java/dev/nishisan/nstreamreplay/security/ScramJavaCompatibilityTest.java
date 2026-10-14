package dev.nishisan.nstreamreplay.security;

import org.apache.kafka.common.security.authenticator.SaslClientCallbackHandler;
import org.apache.kafka.common.security.scram.ScramExtensionsCallback;
import org.apache.kafka.common.security.scram.internals.ScramSaslClient;
import org.apache.kafka.common.security.scram.internals.ScramMechanism;
import org.junit.jupiter.api.Test;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import java.util.List;
import java.util.Map;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class ScramJavaCompatibilityTest {
    @Test
    void scramCallbacksCanReadSubjectOnJava25() throws Exception {
        Subject subject = new Subject();
        subject.getPublicCredentials().add("test-user");
        subject.getPrivateCredentials().add("test-password");
        var handler = new SaslClientCallbackHandler();
        handler.configure(Map.of(), "SCRAM-SHA-512", List.of());
        Subject.callAs(subject, () -> {
            var name = new NameCallback("username");
            var password = new PasswordCallback("password", false);
            handler.handle(new Callback[]{name, password, new ScramExtensionsCallback()});
            assertThat(name.getName()).isEqualTo("test-user");
            assertThat(password.getPassword()).isEqualTo("test-password".toCharArray());
            var client = new ScramSaslClient(ScramMechanism.SCRAM_SHA_512, handler);
            String initial = new String(client.evaluateChallenge(new byte[0]), StandardCharsets.UTF_8);
            assertThat(initial).contains("n=test-user", "r=");
            return null;
        });
    }
}
