package dev.nishisan.nstreamreplay.stats;

import dev.nishisan.utils.spring.stats.StatsUtilsMetricBind;
import dev.nishisan.utils.stats.StatsUtils;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Declara explicitamente o bean {@code statsUtils} e a ponte para o Micrometer.
 *
 * <p><b>Por quê:</b> o {@code NishisanStatsAutoConfiguration} do {@code nishi-utils-spring} cria
 * o {@code statsUtils} sob {@code @ConditionalOnBean(MeterRegistry)}, condição sensível à ordem
 * de avaliação: na prática é avaliada antes do {@code MeterRegistry} do Micrometer existir, então
 * o bean nunca é criado e as métricas não chegam ao {@code /actuator/prometheus}. Aqui o
 * {@link MeterRegistry} é recebido como <b>parâmetro do {@code @Bean}</b> (injeção sob demanda,
 * independente da ordem de processamento), e a ativação é controlada pela propriedade
 * {@code nishi.utils.stats.enabled} — não por {@code @ConditionalOnBean}. A ponte usada é a mesma
 * classe do nishi-utils-spring ({@link StatsUtilsMetricBind}).
 */
@Configuration
@ConditionalOnProperty(name = "nishi.utils.stats.enabled", havingValue = "true", matchIfMissing = false)
public class StatsConfig {

    @Bean
    @ConditionalOnMissingBean(StatsUtils.class)
    public StatsUtils statsUtils(MeterRegistry meterRegistry) {
        StatsUtils statsUtils = new StatsUtils();
        statsUtils.registerListener(new StatsUtilsMetricBind(meterRegistry));
        return statsUtils;
    }
}
