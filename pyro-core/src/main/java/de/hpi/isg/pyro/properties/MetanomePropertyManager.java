package de.hpi.isg.pyro.properties;

import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;

/**
 * Manages a reflectively declared Metanome configuration property.
 *
 * @see MetanomeProperty
 */
public class MetanomePropertyManager {

    private final ConfigurationRequirement<?> configurationRequirement;

    private final Setter setter;

    private final Getter getter;

    public MetanomePropertyManager(ConfigurationRequirement<?> configurationRequirement, Setter setter, Getter getter) {
        this.configurationRequirement = configurationRequirement;
        this.setter = setter;
        this.getter = getter;
    }

    public String getPropertyIdentifier() {
        return this.configurationRequirement.getIdentifier();
    }

    public ConfigurationRequirement<?> getConfigurationRequirement() {
        return this.configurationRequirement;
    }

    public void set(Object algorithm, Object value) throws Exception {
        this.setter.set(algorithm, value);
    }

    public Object get(Object algorithm) throws Exception {
        return this.getter.get(algorithm);
    }

    @FunctionalInterface
    public interface Setter {

        void set(Object algorithm, Object value) throws Exception;

    }

    @FunctionalInterface
    public interface Getter {

        Object get(Object algorithm) throws Exception;

    }

}
