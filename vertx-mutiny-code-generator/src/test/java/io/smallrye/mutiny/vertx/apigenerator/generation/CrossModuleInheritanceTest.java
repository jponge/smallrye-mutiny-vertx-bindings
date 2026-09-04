package io.smallrye.mutiny.vertx.apigenerator.generation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.smallrye.mutiny.vertx.apigenerator.MutinyGenerator;
import io.smallrye.mutiny.vertx.apigenerator.analysis.ShimMethod;
import io.smallrye.mutiny.vertx.apigenerator.tests.Env;

/**
 * Reproducer for cross-module inheritance: when a @VertxGen interface extends another @VertxGen
 * interface from a different Maven module, the generator only has the child's source files —
 * the parent interface is a compiled class on the classpath, not in any source tree.
 *
 * Two bugs are exposed:
 * Bug 1 (VertxGenCollection): inherited methods are silently dropped because the parent's
 * AST MethodDeclaration cannot be found in the compilation units.
 * Bug 2 (HierarchyShimModule): the parent shim is not set as the superclass because
 * the parent is absent from allVertxGenClasses (built from compilation units only).
 *
 * io.vertx.sources.parent.InterfaceWithFuture is a compiled @VertxGen interface that lives in
 * src/test/java but is intentionally NOT added to the Env source tree for these tests.
 */
public class CrossModuleInheritanceTest {

    @Test
    void inheritedMethodsFromExternalVertxGenParentAreGenerated() {
        Env env = new Env();
        env.addJavaCode("io.vertx.sources.child", "ChildApi", """
                package io.vertx.sources.child;

                import io.vertx.codegen.annotations.VertxGen;
                import io.vertx.sources.external.InterfaceWithFuture;

                @VertxGen
                public interface ChildApi extends InterfaceWithFuture {
                    String childMethod();
                }
                """)
                // groupPackage matches the parent's package prefix so shim names can be computed
                .addModuleGen("io.vertx.sources.child", "io.vertx.sources", "child");

        MutinyGenerator generator = new MutinyGenerator(env.root());
        List<MutinyGenerator.GeneratorOutput> outputs = generator.generate();

        var childOutput = Env.getOutputFor(outputs, "io.vertx.sources.child.ChildApi");

        // Bug 1: check ShimClass method registry (Uni / plain variants registered during analysis)
        List<String> shimMethodNames = childOutput.shim().getMethods().stream()
                .map(ShimMethod::getName).toList();
        assertThat(shimMethodNames)
                .as("plainMethod() inherited from cross-module parent must be registered in shim")
                .contains("plainMethod");
        assertThat(shimMethodNames)
                .as("asyncMethod() Uni variant must be registered in shim")
                .contains("asyncMethod");

        // Bug 1: check final generated output (AndAwait / AndForget are emitted directly by generate())
        Env.findMethod(childOutput, "plainMethod");
        Env.findMethod(childOutput, "asyncMethod");
        Env.findMethod(childOutput, "asyncMethodAndAwait");
        Env.findMethod(childOutput, "asyncMethodAndForget");
        Env.findMethod(childOutput, "childMethod");

        // Bug 2 (parent class / shim extension) is verified by the real module build (e.g.
        // vertx-mutiny-auth-jwt) where the parent shim exists on the classpath as a Maven
        // dependency. In this unit test the shim does not exist, so getParentClass() is correctly
        // null — no dangling import to a non-existent class would be emitted.
        assertThat(childOutput.shim().getParentClass())
                .as("parent class must not be set when the parent shim is absent from the classpath")
                .isNull();
    }
}
