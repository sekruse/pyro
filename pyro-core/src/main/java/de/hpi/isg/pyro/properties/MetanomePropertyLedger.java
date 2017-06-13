package de.hpi.isg.pyro.properties;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.configuration.*;
import de.metanome.algorithm_integration.input.FileInputGenerator;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages a set of {@link MetanomeProperty MetanomeProperties}.
 */
public class MetanomePropertyLedger {

    private final Map<String, MetanomePropertyManager> properties = new HashMap<>();

    public void add(MetanomePropertyManager propertyManager) {
        this.properties.put(propertyManager.getPropertyIdentifier(), propertyManager);
    }

    public boolean configure(Object algorithm, String identifier, Object... values) throws AlgorithmConfigurationException {
        MetanomePropertyManager propertyManager = this.properties.get(identifier);
        if (propertyManager == null) return false;

        if (values.length != 1) {
            String msg = String.format(
                    "Reflectively declared Metanome configuration properties allow only for single values (%s -> %s).",
                    identifier, Arrays.toString(values)
            );
            throw new AlgorithmConfigurationException(msg);
        }

        try {
            propertyManager.set(algorithm, values[0]);
        } catch (AlgorithmConfigurationException e) {
            throw e;
        } catch (Exception e) {
            String msg = String.format("Could not set %s to %s.", identifier, values[0]);
            throw new AlgorithmConfigurationException(msg, e);
        }

        return true;
    }

    public void contributeConfigurationRequirements(Collection<ConfigurationRequirement<?>> collector) {
        this.properties.values().stream()
                .map(MetanomePropertyManager::getConfigurationRequirement)
                .forEach(collector::add);
    }

    /**
     * Read the {@link MetanomeProperty} annotations from the given object to come up with a new instance.
     */
    public static MetanomePropertyLedger createFor(Object algorithm) throws AlgorithmConfigurationException {
        MetanomePropertyLedger ledger = new MetanomePropertyLedger();

        Class<?> algorithmClass = algorithm.getClass();
        do {
            Field[] declaredFields = algorithmClass.getDeclaredFields();
            for (Field field : declaredFields) {
                MetanomeProperty metanomeProperty = field.getAnnotation(MetanomeProperty.class);
                if (metanomeProperty == null) continue;

                // Make sure, we can access the field.
                if (!Modifier.isPublic(field.getModifiers())) field.setAccessible(true);

                // Come up with the name of this property.
                final String propertyName = metanomeProperty.name().isEmpty() ? field.getName() : metanomeProperty.name();

                // Inspect the type of the field.
                try {
                    MetanomePropertyManager propertyManager;
                    Class<?> type = field.getType();

                    // "Primitive" types.
                    if (type == boolean.class) {
                        propertyManager = new MetanomePropertyManager(
                                createBooleanRequirement(propertyName, field.getBoolean(algorithm), metanomeProperty.required()),
                                (alg, val) -> field.setBoolean(alg, (Boolean) val),
                                field::getBoolean
                        );
                    } else if (type == Boolean.class) {
                        propertyManager = new MetanomePropertyManager(
                                createBooleanRequirement(propertyName, (Boolean) field.get(algorithm), metanomeProperty.required()),
                                field::set,
                                field::get
                        );
                    } else if (type == int.class) {
                        propertyManager = new MetanomePropertyManager(
                                createIntegerRequirement(propertyName, field.getInt(algorithm), metanomeProperty.required()),
                                (alg, val) -> field.setInt(alg, (Integer) val),
                                field::getInt
                        );
                    } else if (type == Integer.class) {
                        propertyManager = new MetanomePropertyManager(
                                createIntegerRequirement(propertyName, (Integer) field.get(algorithm), metanomeProperty.required()),
                                field::set,
                                field::get
                        );
                    } else if (type == String.class) {
                        propertyManager = new MetanomePropertyManager(
                                createStringRequirement(propertyName, (String) field.get(algorithm), metanomeProperty.required()),
                                field::set,
                                field::get
                        );
                    } else if (type == double.class) {
                        propertyManager = new MetanomePropertyManager(
                                createStringRequirement(propertyName, String.valueOf(field.get(algorithm)), metanomeProperty.required()),
                                (alg, val) -> field.setDouble(algorithm, Double.parseDouble(String.valueOf(val))),
                                field::getDouble
                        );
                    } else if (type == Double.class) {
                        propertyManager = new MetanomePropertyManager(
                                createStringRequirement(propertyName, String.valueOf(field.get(algorithm)), metanomeProperty.required()),
                                (alg, val) -> {
                                    String str = (String) val;
                                    if (str == null || str.isEmpty()) field.set(algorithm, null);
                                    else field.set(algorithm, Double.parseDouble(str));
                                },
                                field::get
                        );

                        // "Complex" types.
                    } else if (type == FileInputGenerator.class) {
                        propertyManager = new MetanomePropertyManager(
                                createFileInputRequirement(propertyName, metanomeProperty.required()),
                                field::set,
                                field::get
                        );

                    } else {
                        throw new UnsupportedOperationException(
                                String.format("Cannot handle field %s of type %s.", field.getName(), field.getType())
                        );
                    }
                    ledger.add(propertyManager);
                } catch (IllegalAccessException e) {
                    throw new AlgorithmConfigurationException("Could not initialize configuration requirements.", e);
                }
            }
            algorithmClass = algorithmClass.getSuperclass();
        } while (algorithmClass != null);

        return ledger;
    }

    private static ConfigurationRequirementBoolean createBooleanRequirement(String name, Boolean defaultValue, boolean isRequired) {
        ConfigurationRequirementBoolean requirement = new ConfigurationRequirementBoolean(name);
        if (defaultValue != null) {
            requirement.setDefaultValues(new Boolean[]{defaultValue});
        }
        requirement.setRequired(isRequired);
        return requirement;
    }

    private static ConfigurationRequirementInteger createIntegerRequirement(String name, Integer defaultValue, boolean isRequired) {
        ConfigurationRequirementInteger requirement = new ConfigurationRequirementInteger(name);
        if (defaultValue != null) {
            requirement.setDefaultValues(new Integer[]{defaultValue});
        }
        requirement.setRequired(isRequired);
        return requirement;
    }

    private static ConfigurationRequirementString createStringRequirement(String name, String defaultValue, boolean isRequired) {
        ConfigurationRequirementString requirement = new ConfigurationRequirementString(name);
        if (defaultValue != null) {
            requirement.setDefaultValues(new String[]{defaultValue});
        }
        requirement.setRequired(isRequired);
        return requirement;
    }

    private static ConfigurationRequirementFileInput createFileInputRequirement(String name, boolean isRequired) {
        ConfigurationRequirementFileInput requirement = new ConfigurationRequirementFileInput(name);
        requirement.setRequired(isRequired);
        return requirement;
    }

    public Map<String, MetanomePropertyManager> getProperties() {
        return this.properties;
    }
}
