package io.smallrye.mutiny.vertx.apigenerator.collection;

import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.symbolsolver.JavaSymbolSolver;

import io.smallrye.mutiny.vertx.apigenerator.utils.ShimConstants;
import io.vertx.codegen.annotations.VertxGen;

public record CollectionResult(
        List<CompilationUnit> units,
        VertxGenModule module,
        List<VertxGenInterface> interfaces,
        List<VertxGenClass> allVertxGenClasses,
        List<String> allDataObjects,
        List<VertxGenModule> allModules,
        JavaSymbolSolver solver) {

    /**
     * Returns true if the given type name is a known {@code @VertxGen} type.
     * <p>
     * Checks the compilation-unit-based registry first. For cross-module types (compiled
     * classes on the classpath that are not in any source tree), falls back to Java reflection,
     * but only if the corresponding mutiny shim class also exists on the classpath — this ensures
     * we never reference a shim that was never generated (e.g. {@code BaseBridgeEvent} whose
     * module is not shimmed in this project).
     */
    public boolean isVertxGen(String name) {
        if (allVertxGenClasses.stream().anyMatch(c -> c.fullyQualifiedName().equals(name))) {
            return true;
        }
        return isCrossModuleVertxGenWithShim(name);
    }

    /**
     * Returns true when {@code name} is a {@code @VertxGen} interface from a cross-module
     * dependency whose mutiny shim class is available on the current classpath.
     * The shim-existence check prevents generating {@code extends} clauses for types whose
     * shimmed module is not a dependency of the module being processed.
     */
    private boolean isCrossModuleVertxGenWithShim(String name) {
        if (name == null || name.contains("<") || name.contains("[") || !name.contains(".")) {
            return false;
        }
        try {
            Class<?> clazz = Class.forName(name);
            if (!clazz.isInterface() || !clazz.isAnnotationPresent(VertxGen.class)) {
                return false;
            }
            // Only accept it if the shim class also exists on the classpath.
            VertxGenModule mod = findModuleByGroupPrefix(name).orElse(null);
            if (mod == null) {
                return false;
            }
            String shimName = ShimConstants.getClassName(mod, name);
            Class.forName(shimName);
            return true;
        } catch (ClassNotFoundException | NoClassDefFoundError | IllegalArgumentException e) {
            return false;
        }
    }

    public VertxGenModule getModuleForVertxGen(String name) {
        return allVertxGenClasses.stream()
                .filter(c -> c.fullyQualifiedName().equals(name))
                .map(VertxGenClass::module)
                .findFirst()
                .orElseGet(() -> findModuleByGroupPrefix(name)
                        .orElseThrow(() -> new NoSuchElementException("Cannot find module for " + name)));
    }

    /**
     * Returns the {@link VertxGenClass} for the given name.
     * For cross-module types not present in the compilation units, a synthetic entry
     * is created using the best-matching module from {@link #allModules} and the
     * {@code concrete} flag read via Java reflection.
     */
    public VertxGenClass getVertxGenClass(String name) {
        return allVertxGenClasses.stream()
                .filter(c -> c.fullyQualifiedName().equals(name))
                .findFirst()
                .orElseGet(() -> createCrossModuleVertxGenClass(name));
    }

    private VertxGenClass createCrossModuleVertxGenClass(String name) {
        VertxGenModule mod = findModuleByGroupPrefix(name)
                .orElseThrow(() -> new NoSuchElementException("Cannot find module for cross-module type: " + name));
        boolean concrete = true;
        try {
            VertxGen ann = Class.forName(name).getAnnotation(VertxGen.class);
            if (ann != null) {
                concrete = ann.concrete();
            }
        } catch (ClassNotFoundException ignored) {
        }
        return new VertxGenClass(name, mod, concrete);
    }

    private java.util.Optional<VertxGenModule> findModuleByGroupPrefix(String fqn) {
        return allModules.stream()
                .filter(m -> fqn.startsWith(m.group()))
                .max(Comparator.comparingInt(m -> m.group().length()));
    }

    public boolean isDataObject(String className) {
        return allDataObjects.contains(className);
    }

    public VertxGenInterface getInterface(String className) {
        return interfaces.stream()
                .filter(i -> i.getFullyQualifiedName().equals(className))
                .findFirst()
                .orElseThrow(() -> new NoSuchElementException("Cannot find interface " + className + " in "
                        + interfaces.stream().map(VertxGenInterface::getFullyQualifiedName).toList()));
    }

    public List<CompilationUnit> allCompilationUnits() {
        return units;
    }
}
