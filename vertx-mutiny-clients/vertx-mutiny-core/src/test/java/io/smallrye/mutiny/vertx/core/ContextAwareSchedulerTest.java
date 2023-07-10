package io.smallrye.mutiny.vertx.core;

import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ContextAwareSchedulerTest {

    Vertx vertx;
    ScheduledExecutorService delegate;

    @Before
    public void setup() {
        vertx = Vertx.vertx();
        delegate = Executors.newSingleThreadScheduledExecutor();
    }

    @After
    public void teardown() {
        delegate.shutdownNow();
        vertx.closeAndAwait();
    }

    @Test
    public void executor_getOrCreateContext_no_context() throws InterruptedException {
        ScheduledExecutorService scheduler = ContextAwareScheduler
                .delegatingTo(delegate)
                .withGetOrCreateContext(vertx);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean ok = new AtomicBoolean();
        scheduler.execute(() -> {
            ok.set(Vertx.currentContext() != null);
            latch.countDown();
        });

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(ok).isTrue();
    }

    @Test
    public void executor_getOrCreateContext_existing_context() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean ok = new AtomicBoolean();
        AtomicReference<Context> expectedContext = new AtomicReference<>();

        vertx.runOnContext(() -> {
            ScheduledExecutorService scheduler = ContextAwareScheduler
                    .delegatingTo(delegate)
                    .withGetOrCreateContext(vertx);
            expectedContext.set(Vertx.currentContext());

            scheduler.execute(() -> {
                Context ctx = Vertx.currentContext();
                ok.set(ctx != null && ctx.getDelegate() != expectedContext.get().getDelegate());
                latch.countDown();
            });
        });

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(ok).isTrue();
    }

    @Test
    public void executor_immediate_getOrCreateContext_no_context() throws InterruptedException {
        ScheduledExecutorService scheduler = ContextAwareScheduler
                .delegatingTo(delegate)
                .withImmediateGetOrCreateContext(vertx);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean ok = new AtomicBoolean();
        scheduler.execute(() -> {
            ok.set(Vertx.currentContext() != null);
            latch.countDown();
        });

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(ok).isTrue();
    }

    @Test
    public void executor_immediate_getOrCreateContext_existing_context() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean ok = new AtomicBoolean();
        AtomicReference<Context> expectedContext = new AtomicReference<>();

        vertx.runOnContext(() -> {
            ScheduledExecutorService scheduler = ContextAwareScheduler
                    .delegatingTo(delegate)
                    .withImmediateGetOrCreateContext(vertx);
            expectedContext.set(Vertx.currentContext());

            scheduler.execute(() -> {
                Context ctx = Vertx.currentContext();
                ok.set(ctx != null && ctx.getDelegate() == expectedContext.get().getDelegate());
                latch.countDown();
            });
        });

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(ok).isTrue();
    }

    @Test
    public void executor_requiredCurrentContext_ok() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean ok = new AtomicBoolean();
        AtomicReference<Context> expectedContext = new AtomicReference<>();

        vertx.runOnContext(() -> {
            ScheduledExecutorService scheduler = ContextAwareScheduler
                    .delegatingTo(delegate)
                    .withRequiredCurrentContext();
            expectedContext.set(Vertx.currentContext());

            scheduler.execute(() -> {
                Context ctx = Vertx.currentContext();
                ok.set(ctx != null && ctx.getDelegate() == expectedContext.get().getDelegate());
                latch.countDown();
            });
        });

        assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(ok).isTrue();
    }

    @Test
    public void executor_requiredCurrentContext_fail() throws InterruptedException {
        assertThatThrownBy(() -> ContextAwareScheduler
                .delegatingTo(delegate)
                .withRequiredCurrentContext())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("There is no Vert.x context in the current thread:");
    }
}
