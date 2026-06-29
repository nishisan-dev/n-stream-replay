package dev.nishisan.nstreamreplay.testutil;

import dev.nishisan.nstreamreplay.model.ReplayRecord;
import dev.nishisan.nstreamreplay.sink.SinkTarget;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/** {@link SinkTarget} de teste que registra os registros enfileirados e as chamadas de sync. */
public final class FakeSinkTarget implements SinkTarget {

    private final String id;
    private final boolean requiresSync;
    public final List<ReplayRecord> received = Collections.synchronizedList(new java.util.ArrayList<>());
    public final AtomicInteger syncs = new AtomicInteger();

    public FakeSinkTarget(String id, boolean requiresSync) {
        this.id = id;
        this.requiresSync = requiresSync;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public boolean offer(ReplayRecord record) {
        received.add(record);
        return true;
    }

    @Override
    public boolean requiresSyncBeforeCommit() {
        return requiresSync;
    }

    @Override
    public void sync() {
        syncs.incrementAndGet();
    }
}
