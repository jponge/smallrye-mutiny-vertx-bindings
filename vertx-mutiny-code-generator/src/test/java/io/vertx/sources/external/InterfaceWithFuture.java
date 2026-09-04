package io.vertx.sources.external;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.Future;

/**
 * A @VertxGen interface with both a Future-returning and a plain method,
 * used to test cross-module inheritance (compiled class, source not in generator path).
 */
@VertxGen
public interface InterfaceWithFuture {
    Future<String> asyncMethod();

    String plainMethod();
}
