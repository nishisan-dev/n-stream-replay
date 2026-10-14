package dev.nishisan.nstreamreplay.config;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import static org.assertj.core.api.Assertions.assertThat;

class DecoderStartupTest {
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(NStreamReplayProperties.class)
    @Import(ConfigValidator.class)
    static class Config { }

    private final ApplicationContextRunner runner = new ApplicationContextRunner().withUserConfiguration(Config.class)
            .withPropertyValues("nstreamreplay.sources[0].id=s", "nstreamreplay.sources[0].bootstrapServers=localhost:9092",
                    "nstreamreplay.sources[0].groupId=g", "nstreamreplay.sinks[0].id=d",
                    "nstreamreplay.sinks[0].bootstrapServers=localhost:9092", "nstreamreplay.routes[0].id=r",
                    "nstreamreplay.routes[0].source=s", "nstreamreplay.routes[0].fromTopic=t",
                    "nstreamreplay.routes[0].to[0].sink=d");

    @Test
    void bootsWithDefaultsAndBothStructuredDecoders() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context.getBean(NStreamReplayProperties.class).sources().get(0).decoder()).isEqualTo("text");
        });
        for (String decoder : new String[]{"json", "yaml"}) {
            runner.withPropertyValues("nstreamreplay.sources[0].decoder=" + decoder,
                    "nstreamreplay.routes[0].to[0].partitionKey=payload.Id")
                    .run(context -> assertThat(context).hasNotFailed());
        }
    }

    @Test
    void invalidConfigurationPreventsStartup() {
        for (String decoder : new String[]{"custom", "unknown", "text"}) {
            runner.withPropertyValues("nstreamreplay.sources[0].decoder=" + decoder,
                    "nstreamreplay.routes[0].to[0].partitionKey=id")
                    .run(context -> assertThat(context).hasFailed());
        }
        for (String path : new String[]{"", "a..b", "a[0]", "a.*"}) {
            runner.withPropertyValues("nstreamreplay.sources[0].decoder=json",
                    "nstreamreplay.routes[0].to[0].partitionKey=" + path)
                    .run(context -> assertThat(context).hasFailed());
        }
        runner.withPropertyValues("nstreamreplay.sources[0].decoder=json",
                "nstreamreplay.routes[0].to[0].partitionKey=id", "nstreamreplay.routes[0].to[1].sink=d")
                .run(context -> assertThat(context).hasFailed());
    }

    @Test
    void routeOverridesSourceAndRejectsInvalidEffectivePolicies() {
        runner.withPropertyValues("nstreamreplay.routes[0].decoder=json",
                "nstreamreplay.routes[0].to[0].partitionKey=id").run(context -> {
                    assertThat(context).hasNotFailed();
                    var props = context.getBean(NStreamReplayProperties.class);
                    assertThat(props.routes().get(0).decoder()).isEqualTo("json");
                    assertThat(props.sources().get(0).decoder()).isEqualTo("text");
                });
        runner.withPropertyValues("nstreamreplay.sources[0].decoder=json", "nstreamreplay.routes[0].decoder=yaml",
                "nstreamreplay.routes[0].to[0].partitionKey=id").run(context -> assertThat(context).hasNotFailed());
        for (String decoder : new String[]{"text", "custom", "unknown", ""}) {
            runner.withPropertyValues("nstreamreplay.sources[0].decoder=json", "nstreamreplay.routes[0].decoder=" + decoder,
                    "nstreamreplay.routes[0].to[0].partitionKey=id").run(context -> assertThat(context).hasFailed());
        }
        runner.withPropertyValues("nstreamreplay.routes[0].decoder=json", "nstreamreplay.routes[0].to[0].partitionKey=id",
                "nstreamreplay.routes[1].id=r2", "nstreamreplay.routes[1].source=s", "nstreamreplay.routes[1].fromTopic=t",
                "nstreamreplay.routes[1].decoder=yaml", "nstreamreplay.routes[1].to[0].sink=d",
                "nstreamreplay.routes[1].to[0].partitionKey=id").run(context -> assertThat(context).hasFailed());
    }
}
