package io.smallrye.mutiny.vertx.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.mutiny.core.Context;
import io.vertx.mutiny.core.Vertx;

public class ContextAwareSchedulerTest {

    ScheduledExecutorService scheduler;
    Vertx vertx;

    @Before
    public void prepare() {
        scheduler = new ContextAwareScheduler();
        vertx = Vertx.vertx();
    }

    @After
    public void cleanup() {
        scheduler.shutdown();
        vertx.closeAndAwait();
    }

    @Test
    public void outside_vertx_context() throws ExecutionException, InterruptedException, TimeoutException {
        AtomicBoolean called = new AtomicBoolean();
        scheduler.schedule(() -> called.set(true), 100, TimeUnit.MILLISECONDS)
                .get(1, TimeUnit.SECONDS);
        assertThat(called).isTrue();
    }

    @Test
    public void regular_submit_outside_vertx_context() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean calledOnVertxThread = new AtomicBoolean();
        scheduler.submit(() -> {
            calledOnVertxThread.set(Vertx.currentContext() != null);
            latch.countDown();
        });
        assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(calledOnVertxThread).isFalse();
    }

    @Test
    public void regular_submit_from_vertx_context() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean calledOnVertxThread = new AtomicBoolean();

        vertx.setTimer(100, tick -> {
            scheduler.submit(() -> {
                calledOnVertxThread.set(Vertx.currentContext() != null);
                latch.countDown();
            });
        });

        assertThat(latch.await(1, TimeUnit.SECONDS)).isTrue();
        assertThat(calledOnVertxThread).isTrue();
    }

    @Test
    public void in_vertx_context() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean calledOnVertxThread = new AtomicBoolean();
        AtomicBoolean sameContext = new AtomicBoolean();

        vertx.setTimer(50, tick -> {
            Context ctx = Vertx.currentContext();
            ctx.putLocal("foo", "bar");
            scheduler.schedule(() -> {
                Context innerCtx = Vertx.currentContext();
                calledOnVertxThread.set(innerCtx != null);
                sameContext.set("bar".equals(innerCtx.getLocal("foo")));
                latch.countDown();
            }, 50, TimeUnit.MILLISECONDS);
        });
        latch.await(1, TimeUnit.SECONDS);
        assertThat(calledOnVertxThread).isTrue();
    }

    @Test
    public void mutiny_infra_integration() {
        try {
            Infrastructure.setDefaultExecutor(scheduler);
            SomeVerticle verticle = new SomeVerticle();
            vertx.deployVerticleAndAwait(verticle);
            assertThat(verticle.result).startsWith("foo=bar: ").contains("vert.x-eventloop-thread-");
        } finally {
            Infrastructure.setDefaultExecutor();
        }
    }

    @Test
    public void mutiny_without_infra_integration() {
        SomeVerticle verticle = new SomeVerticle();
        vertx.deployVerticleAndAwait(verticle);
        assertThat(verticle.result).startsWith("woops: ").doesNotContain("vert.x-eventloop-thread-");
    }

    static class SomeVerticle extends AbstractVerticle {

        String result;

        @Override
        public Uni<Void> asyncStart() {
            Vertx.currentContext().putLocal("foo", "bar");
            return Uni.createFrom().item("foo=")
                    .onItem().delayIt().by(Duration.ofMillis(100))
                    .onItem().transform(str -> {
                        Context innerCtx = Vertx.currentContext();
                        if (innerCtx == null) {
                            return "woops: " + Thread.currentThread().getName();
                        }
                        return str + innerCtx.<String> getLocal("foo") + ": " + Thread.currentThread().getName();
                    })
                    .onItem().invoke(str -> result = str)
                    .replaceWithVoid();
        }
    }
}
